mod config;
mod curve;
mod gui;
mod monitor;
mod pricing;
mod risk;
mod state;
mod storage;
mod trade;
mod wallet;

use config::Config;
use pumpfun::common::types::{Cluster, PriorityFee};
use pumpfun::PumpFun;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Signer;
use std::sync::{Arc, Mutex};

const CONFIG_PATH: &str = "config.toml";

fn main() -> eframe::Result<()> {
    // --- Load config + wallet (sync, before spinning up the async runtime) ---
    let cfg = Config::load_or_default(CONFIG_PATH).expect("failed to load config.toml");
    let keypair = wallet::load_keypair(&cfg).expect(
        "failed to load wallet key - set PUMPSNIPER_PRIVATE_KEY or config.wallet_key_path",
    );
    let wallet_pubkey = keypair.pubkey();

    println!(
        "pump-sniper starting - dry_run={} - wallet={}",
        cfg.dry_run, wallet_pubkey
    );

    let cfg = Arc::new(Mutex::new(cfg));
    let state = state::new_shared_state(cfg.lock().unwrap().dry_run);
    storage::load_into(&state).ok();

    // Own RPC client pointed straight at whatever endpoint is in config.toml
    // (e.g. your Helius URL) - used for balance/price/risk checks throughout
    // the app, independent of whatever the pumpfun crate's own Cluster
    // setting resolves to.
    let http_url = cfg.lock().unwrap().rpc_http_url.clone();
    let rpc = Arc::new(RpcClient::new(http_url));

    // --- pump.fun client ---
    // Confirmed directly against the pumpfun crate's source: `Cluster::new`
    // takes your own http/ws URLs, so the client's buy/sell/subscribe
    // traffic goes through your Helius endpoint - the same one the
    // RpcClient above uses - rather than a public default.
    let (http_url_for_cluster, ws_url_for_cluster) = {
        let c = cfg.lock().unwrap();
        (c.rpc_http_url.clone(), c.rpc_ws_url.clone())
    };
    let priority_fee = PriorityFee {
        unit_limit: Some(cfg.lock().unwrap().priority_fee_compute_unit_limit),
        unit_price: Some(cfg.lock().unwrap().priority_fee_microlamports_per_cu),
    };
    let cluster = Cluster::new(
        http_url_for_cluster,
        ws_url_for_cluster,
        CommitmentConfig::confirmed(),
        priority_fee,
    );
    let client = Arc::new(PumpFun::new(keypair.clone(), cluster));

    // --- tokio runtime for all the background/network work ---
    let rt = tokio::runtime::Runtime::new().expect("failed to start tokio runtime");

    {
        let client = client.clone();
        let rpc = rpc.clone();
        let cfg = cfg.clone();
        let state = state.clone();
        rt.spawn(async move {
            if let Err(e) = monitor::run(client, rpc, cfg, state.clone()).await {
                let mut s = state.lock().unwrap();
                s.push_log(format!("monitor task ended with error: {e}"));
            }
        });
    }

    {
        let client = client.clone();
        let rpc = rpc.clone();
        let cfg = cfg.clone();
        let state = state.clone();
        rt.spawn(async move {
            trade::run_position_manager(client, rpc, cfg, state).await;
        });
    }

    {
        let rpc = rpc.clone();
        let state = state.clone();
        rt.spawn(async move {
            pricing::run(rpc, wallet_pubkey, state).await;
        });
    }

    {
        let state = state.clone();
        rt.spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(15)).await;
                if let Err(e) = storage::save_from(&state) {
                    let mut s = state.lock().unwrap();
                    s.push_log(format!("autosave failed: {e}"));
                }
            }
        });
    }

    // --- GUI (blocks the main thread until the window closes) ---
    let native_options = eframe::NativeOptions::default();
    eframe::run_native(
        "Pump Sniper",
        native_options,
        Box::new(move |_cc| Ok(Box::new(gui::PumpSniperApp::new(state, cfg, CONFIG_PATH.to_string())))),
    )
}
