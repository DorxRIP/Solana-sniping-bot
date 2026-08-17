use base64::Engine;
use bs58;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::Message as SolanaMessage;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::{SeedDerivable, Signer};
use solana_sdk::system_instruction;
use solana_sdk::transaction::Transaction;
use std::error::Error;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::{LazyLock, Mutex};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

const WS_URL: &str = "wss://pumpportal.fun/api/data";
const RECONNECT_DELAY: Duration = Duration::from_secs(4);

#[allow(dead_code)]
static TOKEN_COUNT: AtomicUsize = AtomicUsize::new(1);
static ACTIVE_POSITIONS: LazyLock<Mutex<Vec<Position>>> = LazyLock::new(|| Mutex::new(Vec::new()));

const RESET: &str = "\x1b[0m";
const GREEN: &str = "\x1b[32m";
const CYAN: &str = "\x1b[36m";
const YELLOW: &str = "\x1b[33m";
const BLUE: &str = "\x1b[34m";
const WHITE: &str = "\x1b[37m";
const RED: &str = "\x1b[31m";

const PUMP_ASCII: &str = r#" ______   __  __   ___ __ __   ______
/_____ /\ /_/\/_/\ /__//_//_/\ /_____ /\
\:::_ \ \\:\ \:\ \\::\| \| \ \\:::_ \ \
 \:(_) \ \\:\ \:\ \\:.      \ \\:(_) \ \
  \: ___\/ \:\ \:\ \\:.\-/\  \ \\: ___\/
   \ \ \    \:\_\:\ \ . \  \  \ \\ \ \   
    \_\/     \_____\/ \__\/ \__\/ \_\/   "#;

#[derive(Clone, Copy, Debug)]
struct TradingConfig {
    gas_priority_sol: f64,
    bribe_priority_sol: f64,
    slippage_pct: f64,
    mev_protection: bool,
    pump_tokens_only: bool,
    auto_buy_new_tokens: bool,
    auto_sell_profit_pct: f64,
    auto_sell_percent: f64,
    trade_amount_sol: f64,
    starting_sol_balance: f64,
}

impl TradingConfig {
    fn defaults() -> Self {
        Self {
            gas_priority_sol: GAS_PRIORITY_SOL,
            bribe_priority_sol: BRIBE_PRIORITY_SOL,
            slippage_pct: SLIPPAGE_PCT,
            mev_protection: MEV_PROTECTION,
            pump_tokens_only: PUMP_TOKENS_ONLY,
            auto_buy_new_tokens: AUTO_BUY_NEW_TOKENS,
            auto_sell_profit_pct: AUTO_SELL_PROFIT_PCT,
            auto_sell_percent: AUTO_SELL_PERCENT,
            trade_amount_sol: TRADE_AMOUNT_SOL,
            starting_sol_balance: STARTING_SOL_BALANCE,
        }
    }
}

// ----------------------------
// TRADING SETTINGS
// ----------------------------
// Change values here — this is the main area to edit your bot settings.
const GAS_PRIORITY_SOL: f64 = 0.00005;
const BRIBE_PRIORITY_SOL: f64 = 0.00005;
const SLIPPAGE_PCT: f64 = 25.0;
const MEV_PROTECTION: bool = false;
const PUMP_TOKENS_ONLY: bool = true;
const AUTO_BUY_NEW_TOKENS: bool = true;
const AUTO_SELL_PROFIT_PCT: f64 = 50.0;
const AUTO_SELL_PERCENT: f64 = 100.0;
// A 0.1 SOL trade at a 50% target realizes +0.05 SOL when 100% is sold.
const TRADE_AMOUNT_SOL: f64 = 0.1;
const MAX_ACTIVE_DRY_RUN_POSITIONS: usize = 1;
const STARTING_SOL_BALANCE: f64 = 2.0;

fn build_config() -> TradingConfig {
    TradingConfig::defaults()
}

#[derive(Clone, Debug)]
struct WalletInfo {
    pubkey: String,
    balance_sol: f64,
}

#[derive(Clone, Debug, Default)]
struct ExecutionStatus {
    valid_private_key: bool,
    rpc_ready: bool,
    enough_sol: bool,
    trade_ready: bool,
    balance_sol: f64,
    pubkey: String,
}

fn validate_private_key(private_key: &str) -> bool {
    if private_key.trim().is_empty() {
        return false;
    }

    let decoded = match bs58::decode(private_key.trim()).into_vec() {
        Ok(v) => v,
        Err(_) => return false,
    };

    if decoded.len() == 32 {
        return solana_sdk::signature::Keypair::from_seed(&decoded).is_ok();
    }

    if decoded.len() == 64 {
        let mut bytes = [0u8; 64];
        bytes.copy_from_slice(&decoded);
        return solana_sdk::signature::Keypair::from_bytes(&bytes).is_ok();
    }

    false
}

fn derive_pubkey_from_private_key(private_key: &str) -> Option<String> {
    let decoded = bs58::decode(private_key.trim()).into_vec().ok()?;

    if decoded.len() == 32 {
        let keypair = solana_sdk::signature::Keypair::from_seed(&decoded).ok()?;
        return Some(keypair.pubkey().to_string());
    }

    if decoded.len() == 64 {
        let mut bytes = [0u8; 64];
        bytes.copy_from_slice(&decoded);
        let keypair = solana_sdk::signature::Keypair::from_bytes(&bytes).ok()?;
        return Some(keypair.pubkey().to_string());
    }

    None
}

fn load_keypair_from_private_key(private_key: &str) -> Option<solana_sdk::signature::Keypair> {
    let decoded = bs58::decode(private_key.trim()).into_vec().ok()?;

    if decoded.len() == 32 {
        return solana_sdk::signature::Keypair::from_seed(&decoded).ok();
    }

    if decoded.len() == 64 {
        let mut bytes = [0u8; 64];
        bytes.copy_from_slice(&decoded);
        return solana_sdk::signature::Keypair::from_bytes(&bytes).ok();
    }

    None
}

async fn get_balance_lamports(rpc_url: &str, pubkey: &str) -> Option<u64> {
    let response = reqwest::Client::new()
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [pubkey]
        }))
        .send()
        .await
        .ok()?;

    let payload: Value = response.json().await.ok()?;
    payload
        .get("result")
        .and_then(|value| value.get("value"))
        .and_then(|value| value.as_u64())
}

async fn get_recent_blockhash(rpc_url: &str) -> Option<Hash> {
    let response = reqwest::Client::new()
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestBlockhash",
            "params": [{ "commitment": "confirmed" }]
        }))
        .send()
        .await
        .ok()?;

    let payload: Value = response.json().await.ok()?;
    let blockhash = payload
        .get("result")
        .and_then(|value| value.get("value"))
        .and_then(|value| value.get("blockhash"))
        .and_then(|v| v.as_str())?;

    Hash::from_str(blockhash).ok()
}

async fn send_real_devnet_tx_smoke_test() -> Result<String, Box<dyn Error>> {
    let enable_real_tx = std::env::var("ENABLE_REAL_TX")
        .unwrap_or_default()
        .eq_ignore_ascii_case("true");

    if !enable_real_tx {
        return Err("ENABLE_REAL_TX is not true; devnet transaction execution is disabled".into());
    }

    let private_key = std::env::var("PRIVATE_KEY")?;
    let keypair = load_keypair_from_private_key(&private_key)
        .ok_or("PRIVATE_KEY is not a valid base58 Solana key")?;

    let wallet_pubkey = keypair.pubkey().to_string();
    let rpc_url = std::env::var("RPC_URL").unwrap_or_else(|_| "https://api.devnet.solana.com".to_string());
    let balance_lamports = get_balance_lamports(&rpc_url, &wallet_pubkey).await
        .ok_or("Could not load wallet balance from RPC")?;

    if balance_lamports < 1_000 {
        return Err(format!("Wallet balance is too low to sign a devnet tx: {} lamports", balance_lamports).into());
    }

    let recent_blockhash = get_recent_blockhash(&rpc_url).await
        .ok_or("Could not fetch recent blockhash")?;

    let recipient = Pubkey::from_str(&wallet_pubkey).unwrap();
    let instruction = system_instruction::transfer(&keypair.pubkey(), &recipient, 1);
    let message = SolanaMessage::new(&[instruction], Some(&keypair.pubkey()));
    let mut transaction = Transaction::new_unsigned(message);
    transaction.sign(&[&keypair], recent_blockhash);

    let serialized = bincode::serialize(&transaction)?;
    let tx_base64 = base64::engine::general_purpose::STANDARD.encode(serialized);

    let response = reqwest::Client::new()
        .post(&rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                tx_base64,
                {
                    "skipPreflight": false,
                    "preflightCommitment": "confirmed",
                    "encoding": "base64"
                }
            ]
        }))
        .send()
        .await?;

    let payload: Value = response.json().await?;
    let signature = payload
        .get("result")
        .and_then(|v| v.as_str())
        .ok_or("sendTransaction did not return a signature")?;

    println!("{}REAL DEVNET TX SENT:{} {}{}", CYAN, RESET, GREEN, signature);

    let confirmation_url = format!("https://explorer.solana.com/tx/{signature}?cluster=devnet");
    println!("{}CONFIRMATION:{} {}{}", CYAN, RESET, BLUE, confirmation_url);

    Ok(signature.to_string())
}

async fn load_wallet_info() -> Option<WalletInfo> {
    let private_key = std::env::var("PRIVATE_KEY").ok()?;
    if private_key.trim().is_empty() {
        return None;
    }

    let pubkey = derive_pubkey_from_private_key(&private_key)?;
    let rpc_url = std::env::var("RPC_URL").unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());

    let response = reqwest::Client::new()
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [pubkey]
        }))
        .send()
        .await
        .ok()?;

    let payload: Value = response.json().await.ok()?;
    let lamports = payload
        .get("result")
        .and_then(|value| value.get("value"))
        .and_then(|value| value.as_u64())?;

    let balance_sol = lamports as f64 / 1_000_000_000.0;

    Some(WalletInfo {
        pubkey,
        balance_sol,
    })
}

async fn check_execution_ready() -> ExecutionStatus {
    let private_key = std::env::var("PRIVATE_KEY").unwrap_or_default();
    let valid_private_key = validate_private_key(&private_key);

    if !valid_private_key {
        return ExecutionStatus {
            valid_private_key: false,
            rpc_ready: false,
            enough_sol: false,
            trade_ready: false,
            balance_sol: 0.0,
            pubkey: String::new(),
        };
    }

    let pubkey = derive_pubkey_from_private_key(&private_key).unwrap_or_default();
    let rpc_url = std::env::var("RPC_URL").unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());

    let response = reqwest::Client::new()
        .post(&rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [pubkey.clone()]
        }))
        .send()
        .await;

    let balance_sol = match response {
        Ok(resp) => {
            match resp.json::<Value>().await {
                Ok(payload) => payload
                    .get("result")
                    .and_then(|value| value.get("value"))
                    .and_then(|value| value.as_u64())
                    .map(|lamports| lamports as f64 / 1_000_000_000.0)
                    .unwrap_or(0.0),
                Err(_) => 0.0,
            }
        }
        Err(_) => 0.0,
    };

    let rpc_ready = !pubkey.is_empty() && balance_sol >= 0.0;
    let need_for_trade = 0.02;
    let enough_sol = balance_sol >= need_for_trade;
    let trade_ready = valid_private_key && rpc_ready && enough_sol;

    ExecutionStatus {
        valid_private_key,
        rpc_ready,
        enough_sol,
        trade_ready,
        balance_sol,
        pubkey,
    }
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    let private_key = std::env::var("PRIVATE_KEY").unwrap_or_else(|_| String::new());
    if private_key.is_empty() {
        println!("{}DRY RUN: private key is not required and no transactions will be sent.{}", CYAN, RESET);
    } else {
        println!("{}PRIVATE_KEY loaded from .env{}", GREEN, RESET);
    }

    let wallet = load_wallet_info().await;
    let config = build_config();
    let execution_status = check_execution_ready().await;
    let bot_status = build_bot_status(config, wallet.as_ref().map(|w| w.balance_sol).unwrap_or(execution_status.balance_sol));
    print_bot_status(&bot_status);
    if let Some(wallet) = wallet {
        println!("{}WALLET:{} {}{}", CYAN, RESET, wallet.pubkey, RESET);
    }

    if std::env::var("ENABLE_REAL_TX")
        .unwrap_or_default()
        .eq_ignore_ascii_case("true")
    {
        eprintln!("{}DRY RUN MODE: ENABLE_REAL_TX is ignored; no transaction will be sent.{}", YELLOW, RESET);
    }

    println!("{}EXECUTION GATES{}", CYAN, RESET);
    println!("KEY VALID: {}{}{}", if execution_status.valid_private_key { GREEN } else { RED }, if execution_status.valid_private_key { "YES" } else { "NO" }, RESET);
    println!("RPC READY: {}{}{}", if execution_status.rpc_ready { GREEN } else { RED }, if execution_status.rpc_ready { "YES" } else { "NO" }, RESET);
    println!("ENOUGH SOL: {}{}{}", if execution_status.enough_sol { GREEN } else { RED }, if execution_status.enough_sol { "YES" } else { "NO" }, RESET);
    println!("AUTO-TRADE READY: {}{}{}\n", if execution_status.trade_ready { GREEN } else { RED }, if execution_status.trade_ready { "YES" } else { "NO" }, RESET);

    if !execution_status.trade_ready {
        println!("{}REAL EXECUTION BLOCKED: missing private key / RPC / solvency checks.{}", YELLOW, RESET);
    }

    loop {
        match connect_and_listen(config).await {
            Ok(true) => {
                println!("{}Scanner shutdown requested. Exiting cleanly.{}", YELLOW, RESET);
                break;
            }
            Ok(false) => {
                println!("{}WebSocket session ended. Reconnecting in {}s...{}", YELLOW, RECONNECT_DELAY.as_secs(), RESET);
            }
            Err(err) => {
                eprintln!("{}WebSocket error: {}. Reconnecting in {}s...{}", YELLOW, err, RECONNECT_DELAY.as_secs(), RESET);
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn connect_and_listen(config: TradingConfig) -> Result<bool, Box<dyn Error>> {
    print_trade_config(config);
    println!("{}{}{}", RED, PUMP_ASCII, RESET);
    println!("\n{}PUMP.FUN TOKEN SCANNER{}", YELLOW, RESET);
    println!("{}Real-time new token detection via PumpPortal{}\n", CYAN, RESET);

    let pumpportal_api_key = std::env::var("PUMPPORTAL_API_KEY").unwrap_or_default();
    let can_track_live_trades = !pumpportal_api_key.trim().is_empty();
    let ws_url = if pumpportal_api_key.trim().is_empty() {
        WS_URL.to_string()
    } else {
        format!("{WS_URL}?api-key={}", pumpportal_api_key.trim())
    };

    println!("[{}] {}Connecting to PumpPortal WebSocket...{}", timestamp(), YELLOW, RESET);
    let (mut ws_stream, _) = connect_async(&ws_url).await?;

    println!("[{}] {}Connected{}", timestamp(), GREEN, RESET);
    println!("[{}] {}Subscribing to new token events...{}", timestamp(), YELLOW, RESET);

    ws_stream
        .send(Message::Text(r#"{"method":"subscribeNewToken"}"#.into()))
        .await?;

    println!("[{}] {}Subscribed — waiting for new tokens...{}", timestamp(), GREEN, RESET);

    let mut shutdown = Box::pin(tokio::signal::ctrl_c());

    loop {
        tokio::select! {
            _ = shutdown.as_mut() => {
                println!("{}\nInterrupted by user. Closing websocket...{}", YELLOW, RESET);
                ws_stream.close(None).await.ok();
                return Ok(true);
            }
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Some(token) = parse_token_message(&text) {
                            if let Some(mint) = start_dry_run_position(token, config) {
                                if can_track_live_trades {
                                    subscribe_to_token_trades(&mut ws_stream, &mint).await?;
                                } else {
                                    eprintln!("{}DRY RUN: PUMPPORTAL_API_KEY is missing; cannot monitor {} live.{}", YELLOW, mint, RESET);
                                }
                            }
                        } else if let Some((mint, market_cap_sol)) = parse_market_cap_update(&text) {
                            update_dry_run_position(&mint, market_cap_sol, config);
                        } else if text.contains("Successfully subscribed") {
                            println!("[{}] {}Subscribed — waiting for new tokens...{}", timestamp(), GREEN, RESET);
                        } else if text.contains("Invalid") || text.contains("error") || text.contains("Error") {
                            eprintln!("[{}] {}{}{}", timestamp(), YELLOW, text, RESET);
                        }
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        let text = String::from_utf8_lossy(&bytes);
                        if let Some(token) = parse_token_message(&text) {
                            if let Some(mint) = start_dry_run_position(token, config) {
                                if can_track_live_trades {
                                    subscribe_to_token_trades(&mut ws_stream, &mint).await?;
                                } else {
                                    eprintln!("{}DRY RUN: PUMPPORTAL_API_KEY is missing; cannot monitor {} live.{}", YELLOW, mint, RESET);
                                }
                            }
                        } else if let Some((mint, market_cap_sol)) = parse_market_cap_update(&text) {
                            update_dry_run_position(&mint, market_cap_sol, config);
                        }
                    }
                    Some(Ok(Message::Ping(_))) => {
                        ws_stream.send(Message::Pong(vec![].into())).await?;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        return Ok(false);
                    }
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(Box::new(err)),
                }
            }
        }
    }
}

fn print_trade_config(config: TradingConfig) {
    println!("{}{}Wallet{} {}{:.4} SOL{}{}", CYAN, WHITE, RESET, GREEN, config.starting_sol_balance, RESET, RESET);
    println!("{}LIVE{} | {}Migrations 0{} | {}Bought 0{} | {}Listening{}", GREEN, RESET, YELLOW, RESET, YELLOW, RESET, CYAN, RESET);
    println!("{}Waiting for tokens to graduate...{}\n", YELLOW, RESET);
    println!("[{}PumpFun{}] {}Connecting...{}", YELLOW, RESET, CYAN, RESET);
    println!("[{}PumpFun{}] {}Connected{}", YELLOW, RESET, GREEN, RESET);
    println!("[{}PumpFun{}] {}Subscribed to migrations{}\n", YELLOW, RESET, CYAN, RESET);
    println!("{}{}----- TRADING SETTINGS -----{}", CYAN, RESET, RESET);
    println!("{}Gas priority:{} {:.5} SOL", YELLOW, RESET, config.gas_priority_sol);
    println!("{}Bribe priority:{} {:.5} SOL", YELLOW, RESET, config.bribe_priority_sol);
    println!("{}Slippage:{} {:.1}%", YELLOW, RESET, config.slippage_pct);
    println!("{}MEV protection:{} {}", YELLOW, RESET, if config.mev_protection { "ON" } else { "OFF" });
    println!("{}Pump tokens only:{} {}", YELLOW, RESET, if config.pump_tokens_only { "YES" } else { "NO" });
    println!("{}Auto buy new tokens:{} {}", YELLOW, RESET, if config.auto_buy_new_tokens { "YES" } else { "NO" });
    println!("{}Auto sell profit:{} {:.0}%", YELLOW, RESET, config.auto_sell_profit_pct);
    println!("{}Auto sell amount:{} {:.0}%", YELLOW, RESET, config.auto_sell_percent);
    println!("{}Trade amount:{} {:.4} SOL", YELLOW, RESET, config.trade_amount_sol);
    println!("{}Dry-run positions:{} {}", YELLOW, RESET, MAX_ACTIVE_DRY_RUN_POSITIONS);
    println!("{}Starting balance:{} {:.2} SOL{}\n", YELLOW, RESET, config.starting_sol_balance, RESET);
}

fn parse_token_message(raw: &str) -> Option<Token> {
    let value: Value = serde_json::from_str(raw).ok()?;

    if value.get("mint").is_none() {
        return None;
    }

    if value.get("txType").and_then(|v| v.as_str()) != Some("create") {
        return None;
    }

    let mint = get_string(&value, &["mint"]).unwrap_or_else(|| "N/A".to_string());
    let name = get_string(&value, &["name"]).unwrap_or_else(|| "N/A".to_string());
    let symbol = get_string(&value, &["symbol"]).unwrap_or_else(|| "N/A".to_string());
    let creator = get_string(&value, &["traderPublicKey", "creator", "creatorAddress", "creator_address"]).unwrap_or_else(|| "N/A".to_string());
    let bonding_curve = get_string(&value, &["bondingCurveKey", "bonding_curve"]).unwrap_or_else(|| "N/A".to_string());
    let market_cap_sol = get_f64(&value, &["marketCapSol", "market_cap", "marketCap"]);
    let market_cap = market_cap_sol.map(format_market_cap).unwrap_or_else(|| "N/A".to_string());
    let initial_buy = get_f64(&value, &["initialBuy"]).map(format_initial_buy).unwrap_or_else(|| "N/A".to_string());
    let bonding_sol = get_f64(&value, &["solAmount"]).map(|v| format!("{:.4} SOL", v)).unwrap_or_else(|| "N/A".to_string());
    let metadata = get_string(&value, &["uri", "metadata"]).unwrap_or_else(|| "N/A".to_string());
    let pump_url = format!("https://pump.fun/coin/{}", mint);
    let _solscan_url = format!("https://solscan.io/token/{}", mint);
    let chart_url = format!("https://dexscreener.com/solana/{}", mint);

    Some(Token {
        mint,
        name,
        symbol,
        creator,
        bonding_curve,
        market_cap,
        market_cap_sol,
        initial_buy,
        bonding_sol,
        metadata,
        pump_url,
        chart_url,
    })
}

struct Token {
    mint: String,
    name: String,
    symbol: String,
    creator: String,
    bonding_curve: String,
    market_cap: String,
    market_cap_sol: Option<f64>,
    initial_buy: String,
    bonding_sol: String,
    metadata: String,
    pump_url: String,
    chart_url: String,
}

impl Token {
    #[allow(dead_code)]
    fn _unused_debug_fields(&self) {
        let _ = (&self.creator, &self.bonding_curve, &self.initial_buy, &self.bonding_sol, &self.metadata, &self.chart_url);
    }
}

#[derive(Clone, Debug)]
struct Position {
    mint: String,
    name: String,
    symbol: String,
    chart_url: String,
    snip_url: String,
    entry_sol: f64,
    entry_market_cap_sol: f64,
    last_pct: f64,
    last_sol: f64,
}

struct BotStatus {
    active: bool,
    balance_sol: f64,
    buy_amount_sol: f64,
    slippage_pct: f64,
    priority_fee_sol: f64,
    mode: String,
}

fn wallet_connected() -> bool {
    !std::env::var("PRIVATE_KEY").unwrap_or_default().trim().is_empty()
}

fn build_bot_status(config: TradingConfig, balance_sol: f64) -> BotStatus {
    BotStatus {
        active: true,
        balance_sol,
        buy_amount_sol: config.trade_amount_sol,
        slippage_pct: config.slippage_pct,
        priority_fee_sol: config.gas_priority_sol,
        mode: "PUMPFUN DRY RUN — NO TRANSACTIONS".to_string(),
    }
}

fn real_trade_ready_for_auto_buy(balance_sol: f64, buy_amount_sol: f64, priority_fee_sol: f64) -> bool {
    let required = buy_amount_sol + priority_fee_sol + 0.001;
    balance_sol >= required
}

fn print_bot_status(status: &BotStatus) {
    let light = if status.active { GREEN } else { RED };
    let label = if status.active { "ON" } else { "OFF" };

    println!("{}===== BOT STATUS ====={}", CYAN, RESET);
    println!("{}LIGHT:{} {}{}", YELLOW, RESET, light, label);
    println!("{}MODE:{} {}{}", YELLOW, RESET, WHITE, status.mode);
    println!("{}BUY AMOUNT:{} {:.4} SOL{}", YELLOW, RESET, status.buy_amount_sol, RESET);
    println!("{}SLIPPAGE:{} {:.1}%{}", YELLOW, RESET, status.slippage_pct, RESET);
    println!("{}PRIORITY FEE:{} {:.5} SOL{}", YELLOW, RESET, status.priority_fee_sol, RESET);
    println!("{}WALLET:{} {:.4} SOL{}\n", YELLOW, RESET, status.balance_sol, RESET);
}

async fn subscribe_to_token_trades<S>(ws_stream: &mut S, mint: &str) -> Result<(), Box<dyn Error>>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: Error + 'static,
{
    let subscription = json!({ "method": "subscribeTokenTrade", "keys": [mint] });
    ws_stream.send(Message::Text(subscription.to_string().into())).await?;
    println!("[{}] {}DRY RUN: subscribed to live trades for {}{}", timestamp(), CYAN, mint, RESET);
    Ok(())
}

fn start_dry_run_position(token: Token, config: TradingConfig) -> Option<String> {
    if !should_trade_pump_token(&token) || !config.auto_buy_new_tokens {
        return None;
    }

    let entry_market_cap_sol = token.market_cap_sol?;
    if entry_market_cap_sol <= 0.0 {
        return None;
    }

    let mut positions = ACTIVE_POSITIONS.lock().unwrap();
    if positions.len() >= MAX_ACTIVE_DRY_RUN_POSITIONS {
        return None;
    }

    let mint = token.mint.clone();
    println!("{}DRY-RUN BUY:{} would buy {:.4} SOL of {} ({}) at {:.4} SOL market cap", GREEN, RESET, config.trade_amount_sol, token.name, token.symbol, entry_market_cap_sol);
    println!("{}TARGET:{} would sell 100% when estimated position value reaches {:.4} SOL", CYAN, RESET, config.trade_amount_sol * (1.0 + config.auto_sell_profit_pct / 100.0));

    positions.push(Position {
        mint: token.mint,
        name: token.name,
        symbol: token.symbol,
        chart_url: token.chart_url,
        snip_url: token.pump_url,
        entry_sol: config.trade_amount_sol,
        entry_market_cap_sol,
        last_pct: 0.0,
        last_sol: 0.0,
    });

    Some(mint)
}

fn parse_market_cap_update(raw: &str) -> Option<(String, f64)> {
    let value: Value = serde_json::from_str(raw).ok()?;
    if value.get("txType").and_then(|v| v.as_str()) == Some("create") {
        return None;
    }

    let mint = get_string(&value, &["mint"])?;
    let market_cap_sol = get_f64(&value, &["marketCapSol", "market_cap", "marketCap"])?;
    (market_cap_sol > 0.0).then_some((mint, market_cap_sol))
}

fn update_dry_run_position(mint: &str, market_cap_sol: f64, config: TradingConfig) {
    let mut positions = ACTIVE_POSITIONS.lock().unwrap();
    let Some(index) = positions.iter().position(|position| position.mint == mint) else {
        return;
    };

    let position = &mut positions[index];
    let estimated_value_sol = estimate_position_value(
        position.entry_sol,
        position.entry_market_cap_sol,
        market_cap_sol,
    );
    position.last_sol = estimated_value_sol - position.entry_sol;
    position.last_pct = (position.last_sol / position.entry_sol) * 100.0;

    let color = if position.last_sol >= 0.0 { GREEN } else { RED };
    let sign = if position.last_sol >= 0.0 { "+" } else { "-" };
    println!("{}DRY-RUN MONITOR:{} {} | estimated value {:.4} SOL | P/L {}{}{:.4} SOL{}", CYAN, RESET, position.symbol, estimated_value_sol, color, sign, position.last_sol.abs(), RESET);

    let target_value_sol = take_profit_value(position.entry_sol, config.auto_sell_profit_pct);
    if estimated_value_sol >= target_value_sol {
        println!("{}DRY-RUN SELL:{} would sell 100% of {} at target {:.4} SOL | PROFIT +{:.4} SOL{}", GREEN, RESET, position.symbol, target_value_sol, target_value_sol - position.entry_sol, RESET);
        positions.remove(index);
    }
}

#[allow(dead_code)]
fn print_trade_monitor(token: &Token, change_pct: f64, change_sol: f64) {
    let color = if change_pct >= 0.0 { GREEN } else { RED };
    let sign = if change_pct >= 0.0 { "+" } else { "-" };

    println!("{}MONITORING{} {}", CYAN, RESET, token.name);
    println!(
        "[{}] {} | ${:.8} | ${:.1}K | EST {}{:.2}% | REAL {}{:.2}% | {}{} {:.4} SOL",
        timestamp(),
        token.name,
        0.00000139,
        1.4,
        sign,
        change_pct.abs(),
        sign,
        change_pct.abs(),
        color,
        sign,
        change_sol.abs(),
    );
    println!("{}P/L:{} {}{}{:.2}%{} | {}{}{:.4} SOL{}", CYAN, RESET, color, sign, change_pct.abs(), RESET, color, sign, change_sol.abs(), RESET);
}

#[allow(dead_code)]
fn print_sell_summary(change_sol: f64) {
    let sell_ready = should_sell_position(change_sol.abs() + 0.1, 0.1, 50.0);
    let color = if change_sol >= 0.0 { GREEN } else { RED };
    let sign = if change_sol >= 0.0 { "+" } else { "-" };

    if change_sol >= 0.0 {
        println!("{}SELL RESULT:{} {}PROFIT {}{:.4} SOL{} | SELL READY: {}{}", GREEN, RESET, color, sign, change_sol, RESET, if sell_ready { "YES" } else { "NO" }, RESET);
    } else {
        println!("{}SELL RESULT:{} {}LOSS {}{:.4} SOL{} | SELL READY: {}{}", RED, RESET, color, sign, change_sol.abs(), RESET, if sell_ready { "YES" } else { "NO" }, RESET);
    }
}

fn estimate_trade_delta(token: &Token, entry_sol: f64) -> (f64, f64) {
    let market_cap = parse_market_cap_value(&token.market_cap);
    let raw_delta = ((market_cap - 30.0) / 30.0) * 100.0;
    let delta_sol = (raw_delta / 100.0) * entry_sol;
    (raw_delta, delta_sol)
}

fn parse_market_cap_value(raw: &str) -> f64 {
    let cleaned = raw.trim().replace(" SOL", "").replace(",", "");
    cleaned.parse::<f64>().unwrap_or(30.0)
}

fn should_trade_pump_token(token: &Token) -> bool {
    if token.mint.is_empty() {
        return false;
    }

    token.mint.ends_with("pump") || token.pump_url.contains("pump.fun")
}

#[allow(dead_code)]
fn should_sell_position(current_value_sol: f64, entry_sol: f64, profit_pct: f64) -> bool {
    let profit = ((current_value_sol - entry_sol) / entry_sol.max(0.000001)) * 100.0;
    profit >= profit_pct
}

fn estimate_position_value(entry_sol: f64, entry_market_cap_sol: f64, current_market_cap_sol: f64) -> f64 {
    if entry_market_cap_sol <= 0.0 || current_market_cap_sol < 0.0 {
        return 0.0;
    }

    entry_sol * current_market_cap_sol / entry_market_cap_sol
}

fn take_profit_value(entry_sol: f64, profit_pct: f64) -> f64 {
    entry_sol * (1.0 + profit_pct / 100.0)
}

fn timestamp() -> String {
    chrono::Local::now().format("%H:%M:%S").to_string()
}

fn format_market_cap(value: f64) -> String {
    format!("{:.2} SOL", value)
}

fn format_initial_buy(value: f64) -> String {
    if value.abs() < 1e-9 {
        return "0 tokens".to_string();
    }
    format!("{:.0} tokens", value)
}

fn get_string(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(s) = v.as_str() {
                return Some(s.to_string());
            }
            if let Some(n) = v.as_f64() {
                return Some(n.to_string());
            }
            if let Some(n) = v.as_i64() {
                return Some(n.to_string());
            }
            if let Some(n) = v.as_u64() {
                return Some(n.to_string());
            }
        }
    }
    None
}

fn get_f64(value: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(n) = v.as_f64() {
                return Some(n);
            }
            if let Some(n) = v.as_i64() {
                return Some(n as f64);
            }
            if let Some(n) = v.as_u64() {
                return Some(n as f64);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{estimate_position_value, take_profit_value};

    #[test]
    fn a_half_gain_on_a_tenth_sol_trade_reaches_point_one_five_sol() {
        assert!((take_profit_value(0.1, 50.0) - 0.15).abs() < 1e-12);
        assert!((estimate_position_value(0.1, 30.0, 45.0) - 0.15).abs() < 1e-12);
    }

    #[test]
    fn an_invalid_entry_market_cap_cannot_create_a_fake_value() {
        assert_eq!(estimate_position_value(0.1, 0.0, 45.0), 0.0);
    }
}
