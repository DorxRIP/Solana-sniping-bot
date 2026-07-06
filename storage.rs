use crate::config::Config;
use crate::curve::{estimate_tokens_out, fetch_curve_state, marginal_price_sol};
use crate::state::{Position, SharedState, TradeRecord};
use pumpfun::{common::types::PriorityFee, PumpFun};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{native_token::sol_to_lamports, pubkey::Pubkey, signature::Signer};
use spl_associated_token_account::get_associated_token_address;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Solana's base per-signature network fee, in SOL. A safe approximation
/// for the GUI's fee accounting (item 10) - the exact figure for any single
/// transaction is always visible on-chain via its signature.
const BASE_NETWORK_FEE_SOL: f64 = 0.000005;

fn estimate_priority_fee_sol(cfg: &Config) -> f64 {
    let microlamports =
        cfg.priority_fee_compute_unit_limit as u64 * cfg.priority_fee_microlamports_per_cu;
    (microlamports as f64 / 1_000_000.0) / 1_000_000_000.0
}

fn priority_fee_from(cfg: &Config) -> PriorityFee {
    PriorityFee {
        unit_limit: Some(cfg.priority_fee_compute_unit_limit),
        unit_price: Some(cfg.priority_fee_microlamports_per_cu),
    }
}

/// Buys `cfg.buy_amount_sol` worth of `mint`. In dry-run mode this only
/// estimates a fill against live on-chain reserves and records it as a
/// simulated position; no transaction is sent. Checklist items 3, 5, 9.
pub async fn execute_buy(
    client: Arc<PumpFun>,
    rpc: Arc<RpcClient>,
    cfg: Config,
    state: SharedState,
    mint: Pubkey,
    symbol: String,
    name: String,
    dev_pct: f64,
) {
    let (vsol, vtok, fee_bps) = match fetch_curve_state(&client, &mint).await {
        Ok(v) => v,
        Err(e) => {
            let mut s = state.lock().unwrap();
            s.push_log(format!("could not read bonding curve for {symbol}: {e}"));
            return;
        }
    };

    if cfg.dry_run {
        let tokens_out = estimate_tokens_out(vsol, vtok, fee_bps, cfg.buy_amount_sol);
        if tokens_out <= 0.0 {
            let mut s = state.lock().unwrap();
            s.push_log(format!("[DRY RUN] {symbol}: price estimate came back zero, skipping"));
            return;
        }
        let price = cfg.buy_amount_sol / tokens_out;

        let mut s = state.lock().unwrap();
        s.total_spent_sol += cfg.buy_amount_sol;
        s.push_log(format!(
            "[DRY RUN] would buy {symbol}: {:.4} SOL -> ~{:.0} tokens (~{:.10} SOL/token, dev held {:.1}%)",
            cfg.buy_amount_sol, tokens_out, price, dev_pct
        ));
        s.positions.push(Position {
            mint: mint.to_string(),
            symbol: symbol.clone(),
            name,
            entry_price_sol: price,
            token_amount: tokens_out,
            sol_spent: cfg.buy_amount_sol,
            opened_at: chrono::Utc::now().timestamp(),
            current_price_sol: price,
            dev_holder_pct_at_entry: dev_pct,
        });
        s.history.push(TradeRecord {
            mint: mint.to_string(),
            symbol,
            side: "buy".into(),
            sol_amount: cfg.buy_amount_sol,
            token_amount: tokens_out,
            price_sol: price,
            fees_sol: 0.0,
            tx_signature: None,
            timestamp: chrono::Utc::now().timestamp(),
            reason: "dry_run".into(),
            dry_run: true,
        });
        return;
    }

    // --- LIVE ---
    let amount_lamports = sol_to_lamports(cfg.buy_amount_sol);
    let priority_fee = priority_fee_from(&cfg);

    match client
        .buy(mint, amount_lamports, Some(true), Some(cfg.slippage_bps), Some(priority_fee))
        .await
    {
        Ok(sig) => {
            let payer = client.payer.pubkey();
            let ata = get_associated_token_address(&payer, &mint);
            let token_amount = match rpc.get_token_account_balance(&ata).await {
                Ok(bal) => bal.ui_amount.unwrap_or(0.0),
                Err(_) => 0.0,
            };
            let price = if token_amount > 0.0 {
                cfg.buy_amount_sol / token_amount
            } else {
                0.0
            };
            let fee_sol = BASE_NETWORK_FEE_SOL + estimate_priority_fee_sol(&cfg);

            let mut s = state.lock().unwrap();
            s.total_spent_sol += cfg.buy_amount_sol;
            s.positions.push(Position {
                mint: mint.to_string(),
                symbol: symbol.clone(),
                name,
                entry_price_sol: price,
                token_amount,
                sol_spent: cfg.buy_amount_sol,
                opened_at: chrono::Utc::now().timestamp(),
                current_price_sol: price,
                dev_holder_pct_at_entry: dev_pct,
            });
            s.history.push(TradeRecord {
                mint: mint.to_string(),
                symbol: symbol.clone(),
                side: "buy".into(),
                sol_amount: cfg.buy_amount_sol,
                token_amount,
                price_sol: price,
                fees_sol: fee_sol,
                tx_signature: Some(sig.to_string()),
                timestamp: chrono::Utc::now().timestamp(),
                reason: "manual".into(),
                dry_run: false,
            });
            s.push_log(format!("BOUGHT {symbol}: {:.4} SOL - tx {sig}", cfg.buy_amount_sol));
        }
        Err(e) => {
            let mut s = state.lock().unwrap();
            s.push_log(format!("BUY FAILED for {symbol}: {e}"));
        }
    }
}

/// Sells an entire open position. `reason` is one of "manual", "take_profit",
/// "stop_loss", or "max_hold" and is stored with the trade record.
pub async fn execute_sell(
    client: Arc<PumpFun>,
    rpc: Arc<RpcClient>,
    cfg: Config,
    state: SharedState,
    mint: Pubkey,
    reason: &str,
) {
    let position = {
        let mut s = state.lock().unwrap();
        let mint_str = mint.to_string();
        s.positions
            .iter()
            .position(|p| p.mint == mint_str)
            .map(|i| s.positions.remove(i))
    };
    let Some(position) = position else { return };

    if cfg.dry_run {
        let (vsol, vtok, _fee_bps) = fetch_curve_state(&client, &mint).await.unwrap_or((0, 0, 0));
        let price = marginal_price_sol(vsol, vtok);
        let sol_out = price * position.token_amount;
        let pnl = sol_out - position.sol_spent;

        let mut s = state.lock().unwrap();
        s.daily_realized_pnl_sol += pnl;
        s.push_log(format!(
            "[DRY RUN] would sell {} ({reason}): ~{:.4} SOL back, P&L ~{:.4} SOL",
            position.symbol, sol_out, pnl
        ));
        s.history.push(TradeRecord {
            mint: mint.to_string(),
            symbol: position.symbol.clone(),
            side: "sell".into(),
            sol_amount: sol_out,
            token_amount: position.token_amount,
            price_sol: price,
            fees_sol: 0.0,
            tx_signature: None,
            timestamp: chrono::Utc::now().timestamp(),
            reason: reason.into(),
            dry_run: true,
        });
        return;
    }

    let token_amount_base_units = (position.token_amount * 1_000_000.0).round() as u64;
    let priority_fee = priority_fee_from(&cfg);

    match client
        .sell(mint, Some(token_amount_base_units), Some(cfg.slippage_bps), Some(priority_fee))
        .await
    {
        Ok(sig) => {
            let (vsol, vtok, _fee_bps) = fetch_curve_state(&client, &mint).await.unwrap_or((0, 0, 0));
            let price = marginal_price_sol(vsol, vtok);
            let sol_out = price * position.token_amount;
            let fee_sol = BASE_NETWORK_FEE_SOL + estimate_priority_fee_sol(&cfg);
            let pnl = sol_out - position.sol_spent - fee_sol;

            let mut s = state.lock().unwrap();
            s.daily_realized_pnl_sol += pnl;
            s.history.push(TradeRecord {
                mint: mint.to_string(),
                symbol: position.symbol.clone(),
                side: "sell".into(),
                sol_amount: sol_out,
                token_amount: position.token_amount,
                price_sol: price,
                fees_sol: fee_sol,
                tx_signature: Some(sig.to_string()),
                timestamp: chrono::Utc::now().timestamp(),
                reason: reason.into(),
                dry_run: false,
            });
            s.push_log(format!("SOLD {} ({reason}): tx {sig}", position.symbol));
        }
        Err(e) => {
            let mut s = state.lock().unwrap();
            s.push_log(format!("SELL FAILED for {}: {e}", position.symbol));
            s.positions.push(position); // put it back, nothing was sold
        }
    }
}

/// Background loop: refreshes live prices on open positions and triggers a
/// sell when take-profit, stop-loss, or (if enabled) max hold time fires.
/// Checklist items 6 and 8.
pub async fn run_position_manager(
    client: Arc<PumpFun>,
    rpc: Arc<RpcClient>,
    cfg: Arc<Mutex<Config>>,
    state: SharedState,
) {
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;

        let cfg_snapshot = cfg.lock().unwrap().clone();

        let open_mints: Vec<(String, Pubkey)> = {
            let s = state.lock().unwrap();
            s.positions
                .iter()
                .filter_map(|p| p.mint.parse::<Pubkey>().ok().map(|m| (p.mint.clone(), m)))
                .collect()
        };

        // Refresh current price for every open position first.
        for (mint_str, mint) in &open_mints {
            if let Ok((vsol, vtok, _)) = fetch_curve_state(&client, mint).await {
                let price = marginal_price_sol(vsol, vtok);
                let mut s = state.lock().unwrap();
                if let Some(p) = s.positions.iter_mut().find(|p| &p.mint == mint_str) {
                    p.current_price_sol = price;
                }
            }
        }

        // Then decide who needs to be sold, based on the freshly-updated prices.
        let to_sell: Vec<(Pubkey, String)> = {
            let s = state.lock().unwrap();
            s.positions
                .iter()
                .filter_map(|p| {
                    let pct = p.unrealized_pct();
                    let age = p.age_seconds();
                    let reason = if pct >= cfg_snapshot.take_profit_pct {
                        Some("take_profit")
                    } else if pct <= -cfg_snapshot.stop_loss_pct {
                        Some("stop_loss")
                    } else if cfg_snapshot.max_hold_enabled && age >= cfg_snapshot.max_hold_seconds as i64 {
                        Some("max_hold")
                    } else {
                        None
                    };
                    reason.and_then(|r| p.mint.parse::<Pubkey>().ok().map(|m| (m, r.to_string())))
                })
                .collect()
        };

        for (mint, reason) in to_sell {
            execute_sell(client.clone(), rpc.clone(), cfg_snapshot.clone(), state.clone(), mint, &reason).await;
        }
    }
}
