use serde::{Deserialize, Serialize};
use std::path::Path;

/// All user-tunable knobs for the bot. Loaded from config.toml at startup and
/// editable live from the GUI's Settings tab (changes are saved back to disk).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    // --- Connectivity ---
    /// HTTPS RPC endpoint, e.g. your Helius URL with API key.
    pub rpc_http_url: String,
    /// WebSocket RPC endpoint (usually same host, wss:// scheme).
    pub rpc_ws_url: String,

    // --- Safety switch ---
    /// When true (the default), the bot detects tokens and logs what it
    /// *would* do, but never sends a real transaction. Flip this off only
    /// once you've watched dry-run behavior and are comfortable with it.
    pub dry_run: bool,

    // --- Trade sizing (Checklist items 3, 4) ---
    /// SOL spent per buy.
    pub buy_amount_sol: f64,
    /// Bot stops opening new positions once total SOL spent (this session)
    /// reaches this cap.
    pub max_total_sol_cap: f64,

    // --- Slippage (item 5) ---
    /// Max acceptable slippage, in basis points (500 = 5%). Enforced
    /// on-chain by the pump.fun program itself, so a worse-than-expected
    /// price simply fails instead of silently executing badly.
    pub slippage_bps: u64,

    // --- Exit rules (item 6) ---
    /// Sell when unrealized gain reaches this percent (0-100+, e.g. 50 = +50%).
    pub take_profit_pct: f64,
    /// Sell when unrealized loss reaches this percent (0-100, e.g. 20 = -20%).
    pub stop_loss_pct: f64,

    // --- Dev/creator holder filter (item 7) ---
    /// Skip the buy if the token creator holds more than this percent of
    /// total supply at detection time.
    pub max_dev_holder_pct: f64,

    // --- Max hold time (item 8) ---
    pub max_hold_enabled: bool,
    pub max_hold_seconds: u64,

    // --- Fees (item 10) ---
    /// Fixed lamports added to every tx as a priority fee tip, on top of
    /// pump.fun's own platform fee (read from the program) and the base
    /// Solana network fee. All three are subtracted when computing P&L.
    pub priority_fee_microlamports_per_cu: u64,
    pub priority_fee_compute_unit_limit: u32,

    // --- Wallet (never put a raw key in this file) ---
    /// Optional path to a local file containing your base58 private key.
    /// If not set, the bot reads the PUMPSNIPER_PRIVATE_KEY env var instead.
    pub wallet_key_path: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rpc_http_url: "https://api.mainnet-beta.solana.com".to_string(),
            rpc_ws_url: "wss://api.mainnet-beta.solana.com".to_string(),
            dry_run: true,
            buy_amount_sol: 0.02,
            max_total_sol_cap: 0.5,
            slippage_bps: 500,
            take_profit_pct: 50.0,
            stop_loss_pct: 20.0,
            max_dev_holder_pct: 10.0,
            max_hold_enabled: true,
            max_hold_seconds: 300,
            priority_fee_microlamports_per_cu: 200_000,
            priority_fee_compute_unit_limit: 200_000,
            wallet_key_path: None,
        }
    }
}

impl Config {
    pub fn load_or_default(path: &str) -> anyhow::Result<Self> {
        if Path::new(path).exists() {
            let raw = std::fs::read_to_string(path)?;
            let cfg: Config = toml::from_str(&raw)?;
            Ok(cfg)
        } else {
            let cfg = Config::default();
            cfg.save(path)?;
            Ok(cfg)
        }
    }

    pub fn save(&self, path: &str) -> anyhow::Result<()> {
        let raw = toml::to_string_pretty(self)?;
        std::fs::write(path, raw)?;
        Ok(())
    }
}
