use crate::state::SharedState;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use std::time::Duration;

/// Polls SOL balance (via your RPC) and the SOL->GBP price (via CoinGecko's
/// free public endpoint) every 30 seconds and writes them into shared state
/// for the GUI's balance panel (checklist item 11).
pub async fn run(rpc: Arc<RpcClient>, wallet_pubkey: Pubkey, state: SharedState) {
    let http = reqwest::Client::new();
    loop {
        // --- SOL balance ---
        match rpc.get_balance(&wallet_pubkey).await {
            Ok(lamports) => {
                let sol = lamports as f64 / 1_000_000_000.0;
                let mut s = state.lock().unwrap();
                s.sol_balance = sol;
            }
            Err(e) => {
                let mut s = state.lock().unwrap();
                s.push_log(format!("balance check failed: {e}"));
            }
        }

        // --- SOL -> GBP price ---
        // If CoinGecko's response shape has changed, this is the one place
        // to fix it - check https://www.coingecko.com/en/api/documentation
        match fetch_sol_gbp(&http).await {
            Ok(price) => {
                let mut s = state.lock().unwrap();
                s.gbp_per_sol = price;
            }
            Err(e) => {
                let mut s = state.lock().unwrap();
                s.push_log(format!("price fetch failed: {e}"));
            }
        }

        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

async fn fetch_sol_gbp(http: &reqwest::Client) -> anyhow::Result<f64> {
    let url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=gbp";
    let resp: serde_json::Value = http.get(url).send().await?.json().await?;
    let price = resp["solana"]["gbp"]
        .as_f64()
        .ok_or_else(|| anyhow::anyhow!("unexpected CoinGecko response shape"))?;
    Ok(price)
}
