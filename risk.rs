use crate::config::Config;
use crate::state::{SharedState, WatchedToken};
use pumpfun::{common::stream::PumpFunEvent, PumpFun};
use solana_client::nonblocking::rpc_client::RpcClient;
use std::sync::{Arc, Mutex};

/// Subscribes to pump.fun's on-chain event stream (checklist item 1: new
/// tokens are picked up the moment the "Create" instruction lands - the
/// only extra latency is your RPC/WebSocket provider's own delivery speed)
/// and runs every new token through the buy-gate filters.
pub async fn run(
    client: Arc<PumpFun>,
    rpc: Arc<RpcClient>,
    cfg: Arc<Mutex<Config>>,
    state: SharedState,
) -> anyhow::Result<()> {
    let client_cb = client.clone();
    let rpc_cb = rpc.clone();
    let cfg_cb = cfg.clone();
    let state_cb = state.clone();

    // Kept alive for the lifetime of this function (see the pending() await
    // below) so the WebSocket subscription stays open.
    let _subscription = client
        .subscribe(None, None, move |_signature, event, error, _resp| {
            if let Some(err) = error {
                let mut s = state_cb.lock().unwrap();
                s.push_log(format!("event stream parse error: {err}"));
                return;
            }
            if let Some(PumpFunEvent::Create(create_event)) = event {
                let client = client_cb.clone();
                let rpc = rpc_cb.clone();
                let cfg = cfg_cb.clone();
                let state = state_cb.clone();
                tokio::spawn(async move {
                    handle_new_token(client, rpc, cfg, state, create_event).await;
                });
            }
        })
        .await?;

    {
        let mut s = state.lock().unwrap();
        s.push_log("Connected - watching pump.fun for new tokens.".to_string());
    }

    std::future::pending::<()>().await;
    #[allow(unreachable_code)]
    Ok(())
}

async fn handle_new_token(
    client: Arc<PumpFun>,
    rpc: Arc<RpcClient>,
    cfg: Arc<Mutex<Config>>,
    state: SharedState,
    create_event: pumpfun::common::stream::CreateEvent,
) {
    let cfg_snapshot = cfg.lock().unwrap().clone();
    let mint = create_event.mint;
    let symbol = create_event.symbol.clone();
    let name = create_event.name.clone();

    let mut watched = WatchedToken {
        mint: mint.to_string(),
        symbol: symbol.clone(),
        name: name.clone(),
        detected_at: chrono::Utc::now().timestamp(),
        dev_holder_pct: None,
        skipped_reason: None,
        bought: false,
    };

    // Gate: bot must be toggled on (checklist item 12).
    let is_running = {
        let s = state.lock().unwrap();
        s.running
    };
    if !is_running {
        watched.skipped_reason = Some("bot is stopped".into());
        let mut s = state.lock().unwrap();
        s.push_watched(watched);
        return;
    }

    // Gate: total SOL spend cap (checklist item 4).
    let cap_ok = {
        let s = state.lock().unwrap();
        s.total_spent_sol + cfg_snapshot.buy_amount_sol <= cfg_snapshot.max_total_sol_cap
    };
    if !cap_ok {
        watched.skipped_reason = Some("max SOL cap reached".into());
        let mut s = state.lock().unwrap();
        s.push_watched(watched);
        return;
    }

    // Gate: this bot only supports the legacy SPL Token program. pump.fun
    // has offered a Token-2022 creation path since late 2025, and the
    // pumpfun crate underneath this bot doesn't handle it - skip cleanly
    // rather than risk a mis-derived account or a doomed transaction.
    match crate::risk::is_legacy_spl_token(&rpc, &mint).await {
        Ok(true) => {}
        Ok(false) => {
            watched.skipped_reason = Some("Token-2022 mint (unsupported)".into());
            let mut s = state.lock().unwrap();
            s.push_watched(watched);
            return;
        }
        Err(e) => {
            watched.skipped_reason = Some("could not check token program (skipped for safety)".into());
            let mut s = state.lock().unwrap();
            s.push_log(format!("token-program check failed for {symbol}: {e}"));
            s.push_watched(watched);
            return;
        }
    }

    // Gate: dev/creator holding percentage (checklist item 7).
    let dev_pct = match crate::risk::dev_holder_pct(&client, &rpc, &mint).await {
        Ok(pct) => pct,
        Err(e) => {
            watched.skipped_reason = Some("dev-holder check failed (skipped for safety)".into());
            let mut s = state.lock().unwrap();
            s.push_log(format!("dev-holder check failed for {symbol}: {e}"));
            s.push_watched(watched);
            return;
        }
    };
    watched.dev_holder_pct = Some(dev_pct);

    if dev_pct > cfg_snapshot.max_dev_holder_pct {
        watched.skipped_reason = Some(format!(
            "dev holds {dev_pct:.1}% (limit {:.1}%)",
            cfg_snapshot.max_dev_holder_pct
        ));
        let mut s = state.lock().unwrap();
        s.push_watched(watched);
        return;
    }

    watched.bought = true;
    {
        let mut s = state.lock().unwrap();
        s.push_watched(watched);
    }

    crate::trade::execute_buy(client, rpc, cfg_snapshot, state, mint, symbol, name, dev_pct).await;
}
