// Minimal Pump.fun sniper.
// Uses PumpPortal WebSocket for new-token, migration, and trade data.
// Uses PumpPortal Local Transaction API for transaction construction.
// PRIVATE_KEY, RPC_URL and PUMPPORTAL_API_KEY stay in .env.
//
// Trade-behavior settings (AUTO_BUY_NEW_TOKENS, TRADE_AMOUNT_SOL, etc.) are
// re-read from the .env file on disk every ~750ms. Edit and save the file
// while the bot is running and changes apply automatically — no restart
// needed. PRIVATE_KEY / RPC_URL / PUMPPORTAL_API_KEY are NOT hot-reloaded
// (changing those still needs a restart, since they affect the live
// websocket connection and signing key).

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use solana_sdk::{
    signature::{Keypair, SeedDerivable, Signer},
    transaction::VersionedTransaction,
};
use std::{
    collections::{HashMap, HashSet},
    env,
    error::Error,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{self, AsyncBufReadExt, BufReader},
    sync::{mpsc, Mutex, RwLock},
    time::sleep,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const PUMPPORTAL_WS: &str = "wss://pumpportal.fun/api/data";
const PUMPPORTAL_LOCAL_TX: &str = "https://pumpportal.fun/api/trade-local";
const JITO_BUNDLE_URL: &str = "https://mainnet.block-engine.jito.wtf/api/v1/bundles";

const AUTO_BUY_DEFAULT: bool = true;
const TRADE_AMOUNT_DEFAULT: f64 = 0.03;
const MAX_TOKEN_AGE_DEFAULT: u64 = 1_000;
const AUTO_SELL_PROFIT_DEFAULT: f64 = 25.0;
const SLIPPAGE_DEFAULT: f64 = 25.0;
const PRIORITY_FEE_DEFAULT: f64 = 0.0002;
const MEV_PROTECTION_DEFAULT: bool = false;
const PUMP_ONLY_DEFAULT: bool = true;
const MIN_SOL_RESERVE_DEFAULT: f64 = 0.01;

// ----------------------------
// TERMINAL STYLING
// ----------------------------

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const UNDERLINE: &str = "\x1b[4m";

const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const YELLOW: &str = "\x1b[33m";
const CYAN: &str = "\x1b[36m";

// ----------------------------
// TOKEN LAYOUT SETTINGS
// ----------------------------

// Main width of token cards.
// Long links automatically expand the box.
const TOKEN_BOX_WIDTH: usize = 93;

const MIGRATION_TITLE: &str = "🎯 MIGRATION";
const PLATFORM_TITLE: &str = "🟢 PUMP.FUN";

const MONITOR_LINE: &str =
    "─────────────────────────────────────────────────────────────────────────────────────────────────────";

// ----------------------------
// STARTUP ASCII
// ----------------------------

const PUMP_ASCII: &str = r#" 
 ______   __  __   ___ __ __   ______    
/_____/\ /_/\/_/\ /__//_//_/\ /_____/\   
\:::_ \ \\:\ \:\ \\::\| \| \ \\:::_ \ \  
 \:(_) \ \\:\ \:\ \\:.      \ \\:(_) \ \ 
  \: ___\/ \:\ \:\ \\:.\-/\  \ \\: ___\/ 
   \ \ \    \:\_\:\ \\. \  \  \ \\ \ \   
    \_\/     \_____\/ \__\/ \__\/ \_\/   
                                          "#;

/// Settings that stay fixed for the life of the process.
#[derive(Clone)]
struct Config {
    rpc_url: String,
    api_key: String,
    auto_buy: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
}

/// Settings that can change while the bot is running, reloaded from .env.
#[derive(Clone, Copy, Debug, PartialEq)]
struct HotSettings {
    trade_amount_sol: f64,
    max_token_age_ms: u64,
    auto_sell_profit_pct: f64,
    slippage_pct: f64,
    priority_fee_sol: f64,
    mev_protection: bool,
    pump_tokens_only: bool,
    min_sol_reserve: f64,
}

#[derive(Clone)]
struct App {
    config: Config,
    http: reqwest::Client,
    keypair: Arc<Keypair>,
    ws_tx: Arc<Mutex<Option<mpsc::UnboundedSender<Message>>>>,
    positions: Arc<RwLock<HashMap<String, Position>>>,
    seen: Arc<Mutex<HashSet<String>>>,
    migrated: Arc<Mutex<HashSet<String>>>,
    hot: Arc<RwLock<HotSettings>>,
    bought_count: Arc<AtomicU64>,
    migration_count: Arc<AtomicU64>,
    sol_balance: Arc<RwLock<Option<f64>>>,
    env_path: Arc<PathBuf>,
    print_lock: Arc<Mutex<()>>,
}

#[derive(Clone)]
struct Position {
    mint: String,
    entry_price_sol_per_token: f64,
    selling: bool,
}

#[derive(Clone)]
struct NewToken {
    mint: String,
    name: String,
    symbol: String,
    received_at: Instant,
    source_timestamp_ms: Option<u64>,
}

#[derive(Clone)]
struct TradeEvent {
    mint: String,
    tx_type: String,
    trader: String,
    sol_amount: f64,
    token_amount: f64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let env_path: PathBuf = dotenv::dotenv().unwrap_or_else(|_| PathBuf::from(".env"));

    let private_key = env::var("PRIVATE_KEY")
        .map_err(|_| "PRIVATE_KEY is missing from .env")?;
    let keypair = load_keypair(&private_key)?;

    let rpc_url = env::var("RPC_URL")
        .or_else(|_| env::var("SOLANA_RPC_URL"))
        .map_err(|_| "RPC_URL or SOLANA_RPC_URL is missing from .env")?;

    let api_key = env::var("PUMPPORTAL_API_KEY")
        .map_err(|_| "PUMPPORTAL_API_KEY is required for live auto-buy and auto-sell tracking")?;

    let auto_buy = Arc::new(AtomicBool::new(
        env_bool("AUTO_BUY_NEW_TOKENS", AUTO_BUY_DEFAULT),
    ));
    let shutdown = Arc::new(AtomicBool::new(false));

    let config = Config {
        rpc_url,
        api_key,
        auto_buy: auto_buy.clone(),
        shutdown: shutdown.clone(),
    };

    let initial_settings = HotSettings {
        trade_amount_sol: env_f64("TRADE_AMOUNT_SOL", TRADE_AMOUNT_DEFAULT),
        max_token_age_ms: env_u64("MAX_TOKEN_AGE_MS", MAX_TOKEN_AGE_DEFAULT),
        auto_sell_profit_pct: env_f64("AUTO_SELL_PROFIT_PCT", AUTO_SELL_PROFIT_DEFAULT),
        slippage_pct: env_f64("SLIPPAGE_PCT", SLIPPAGE_DEFAULT),
        priority_fee_sol: env_f64("PRIORITY_FEE_SOL", PRIORITY_FEE_DEFAULT),
        mev_protection: env_bool("MEV_PROTECTION", MEV_PROTECTION_DEFAULT),
        pump_tokens_only: env_bool("PUMP_TOKENS_ONLY", PUMP_ONLY_DEFAULT),
        min_sol_reserve: env_f64("MIN_SOL_RESERVE", MIN_SOL_RESERVE_DEFAULT),
    };

    if initial_settings.trade_amount_sol <= 0.0 {
        return Err("TRADE_AMOUNT_SOL must be > 0".into());
    }

    if initial_settings.slippage_pct <= 0.0 || initial_settings.slippage_pct > 100.0 {
        return Err("SLIPPAGE_PCT must be between 0 and 100".into());
    }

    if initial_settings.auto_sell_profit_pct < 0.0 {
        return Err("AUTO_SELL_PROFIT_PCT must be >= 0".into());
    }

    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_secs(2))
        .pool_max_idle_per_host(32)
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;

    let app = App {
        config,
        http,
        keypair: Arc::new(keypair),
        ws_tx: Arc::new(Mutex::new(None)),
        positions: Arc::new(RwLock::new(HashMap::new())),
        seen: Arc::new(Mutex::new(HashSet::new())),
        migrated: Arc::new(Mutex::new(HashSet::new())),
        hot: Arc::new(RwLock::new(initial_settings)),
        bought_count: Arc::new(AtomicU64::new(0)),
        migration_count: Arc::new(AtomicU64::new(0)),
        sol_balance: Arc::new(RwLock::new(None)),
        env_path: Arc::new(env_path),
        print_lock: Arc::new(Mutex::new(())),
    };

    print_startup(&app).await?;

    let command_app = app.clone();
    tokio::spawn(async move {
        command_loop(command_app).await;
    });

    let settings_app = app.clone();
    tokio::spawn(async move {
        settings_watcher(settings_app).await;
    });

    let balance_app = app.clone();
    tokio::spawn(async move {
        balance_watcher(balance_app).await;
    });

    scanner_loop(app).await
}

async fn scanner_loop(app: App) -> Result<(), Box<dyn Error + Send + Sync>> {
    loop {
        if app.config.shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        match scanner_session(app.clone()).await {
            Ok(()) => {}
            Err(err) => {
                log_line(&app, &format!("{RED}[WS]{RESET} {err}")).await;
            }
        }

        if !app.config.shutdown.load(Ordering::Relaxed) {
            sleep(Duration::from_millis(250)).await;
        }
    }
}

async fn scanner_session(app: App) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!("{}?api-key={}", PUMPPORTAL_WS, app.config.api_key);

    let (ws, _) = connect_async(url).await?;
    let (mut sink, mut stream) = ws.split();

    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    {
        let mut guard = app.ws_tx.lock().await;
        *guard = Some(tx.clone());
    }

    tx.send(Message::Text(
        json!({"method":"subscribeNewToken"}).to_string().into(),
    ))?;

    // Needed for the migration counter / cards below — without this
    // subscription "migrate" events never arrive on this connection.
    tx.send(Message::Text(
        json!({"method":"subscribeMigration"}).to_string().into(),
    ))?;

    tx.send(Message::Text(
        json!({
            "method":"subscribeAccountTrade",
            "keys":[app.keypair.pubkey().to_string()]
        })
        .to_string()
        .into(),
    ))?;

    let writer = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            if sink.send(message).await.is_err() {
                break;
            }
        }
    });

    log_line(&app, &format!("{GREEN}[WS] connected{RESET}")).await;

    while !app.config.shutdown.load(Ordering::Relaxed) {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => {
                handle_ws_message(&app, &text).await;
            }

            Some(Ok(Message::Binary(bytes))) => {
                if let Ok(text) = std::str::from_utf8(&bytes) {
                    handle_ws_message(&app, text).await;
                }
            }

            Some(Ok(Message::Ping(data))) => {
                let sender = app.ws_tx.lock().await.clone();
                if let Some(sender) = sender {
                    let _ = sender.send(Message::Pong(data));
                }
            }

            Some(Ok(Message::Close(_))) | None => break,

            Some(Err(err)) => {
                writer.abort();
                let mut guard = app.ws_tx.lock().await;
                *guard = None;
                return Err(Box::new(err));
            }

            _ => {}
        }
    }

    writer.abort();

    let mut guard = app.ws_tx.lock().await;
    *guard = None;

    Ok(())
}

async fn handle_ws_message(app: &App, raw: &str) {
    let value: Value = match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(_) => return,
    };

    if value.get("txType").and_then(Value::as_str) == Some("migrate") {
        if let Some(mint) = string_field(&value, "mint") {
            app.migrated.lock().await.insert(mint.clone());
            app.migration_count.fetch_add(1, Ordering::Relaxed);

            let name = string_field(&value, "name").unwrap_or_else(|| "Migrated token".to_string());
            let symbol = string_field(&value, "symbol").unwrap_or_else(|| "?".to_string());

            log_line(app, &token_card("MIGRATION", GREEN, &name, &symbol, &mint)).await;
        }
        return;
    }

    if let Some(token) = parse_new_token(&value) {
        let app = app.clone();
        tokio::spawn(async move {
            buy_new_token(app, token).await;
        });
        return;
    }

    if let Some(trade) = parse_trade_event(&value) {
        let app = app.clone();
        tokio::spawn(async move {
            process_trade(app, trade).await;
        });
    }
}

async fn buy_new_token(app: App, token: NewToken) {
    if !app.config.auto_buy.load(Ordering::Relaxed) {
        return;
    }

    let hot = *app.hot.read().await;

    if hot.pump_tokens_only && !token.mint.ends_with("pump") {
        return;
    }

    if !is_fresh(&token, hot.max_token_age_ms) {
        return;
    }

    {
        let mut seen = app.seen.lock().await;
        if !seen.insert(token.mint.clone()) {
            return;
        }
    }

    // Re-check after the async dedupe lock in case auto-buy was flipped
    // off in the meantime.
    if !app.config.auto_buy.load(Ordering::Relaxed) {
        return;
    }

    let received_ms = token.received_at.elapsed().as_millis();

    log_line(&app, &token_card("NEW TOKEN", YELLOW, &token.name, &token.symbol, &token.mint)).await;
    log_line(
        &app,
        &format!(
            "{DIM}[BUY]{RESET} {} · seen {received_ms}ms ago · buying {:.4} SOL...",
            token.mint, hot.trade_amount_sol
        ),
    )
    .await;

    match execute_trade(&app, "buy", &token.mint, hot.trade_amount_sol, true).await {
        Ok(signature) => {
            app.bought_count.fetch_add(1, Ordering::Relaxed);
            let tx_url = format!("https://solscan.io/tx/{signature}");

            log_line(
                &app,
                &format!(
                    "{GREEN}{BOLD}✓ SNIPED{RESET} {}\n  └ {CYAN}{UNDERLINE}{}{RESET}",
                    token.mint,
                    hyperlink(&tx_url, &tx_url)
                ),
            )
            .await;

            subscribe_token(&app, &token.mint).await;
        }

        Err(err) => {
            log_line(
                &app,
                &format!("{RED}{BOLD}✗ BUY FAILED{RESET} {} — {err}", token.mint),
            )
            .await;
        }
    }
}

async fn process_trade(app: App, trade: TradeEvent) {
    let wallet = app.keypair.pubkey().to_string();

    if trade.trader == wallet
        && trade.tx_type.eq_ignore_ascii_case("buy")
        && trade.token_amount > 0.0
        && trade.sol_amount > 0.0
    {
        let entry_price = trade.sol_amount / trade.token_amount;

        {
            let mut positions = app.positions.write().await;

            positions.insert(
                trade.mint.clone(),
                Position {
                    mint: trade.mint.clone(),
                    entry_price_sol_per_token: entry_price,
                    selling: false,
                },
            );
        }

        log_line(
            &app,
            &format!("{DIM}[TRACK]{RESET} {} entry {:.12} SOL/token", trade.mint, entry_price),
        )
        .await;

        return;
    }

    let position = {
        let positions = app.positions.read().await;
        positions.get(&trade.mint).cloned()
    };

    let Some(position) = position else {
        return;
    };

    if position.selling
        || trade.token_amount <= 0.0
        || trade.sol_amount <= 0.0
    {
        return;
    }

    let price = trade.sol_amount / trade.token_amount;
    let pnl = ((price / position.entry_price_sol_per_token) - 1.0) * 100.0;

    let hot = *app.hot.read().await;

    if pnl < hot.auto_sell_profit_pct {
        return;
    }

    {
        let mut positions = app.positions.write().await;

        let Some(pos) = positions.get_mut(&trade.mint) else {
            return;
        };

        if pos.selling {
            return;
        }

        pos.selling = true;
    }

    log_line(
        &app,
        &format!("{YELLOW}[SELL]{RESET} {} +{:.2}% · selling 100%...", position.mint, pnl),
    )
    .await;

    match execute_trade(&app, "sell", &position.mint, 100.0, false).await {
        Ok(signature) => {
            let tx_url = format!("https://solscan.io/tx/{signature}");

            log_line(
                &app,
                &format!(
                    "{GREEN}{BOLD}✓ SOLD{RESET} {} +{:.2}%\n  └ {CYAN}{UNDERLINE}{}{RESET}",
                    position.mint,
                    pnl,
                    hyperlink(&tx_url, &tx_url)
                ),
            )
            .await;

            app.positions.write().await.remove(&position.mint);
            unsubscribe_token(&app, &position.mint).await;
        }

        Err(err) => {
            log_line(
                &app,
                &format!("{RED}{BOLD}✗ SELL FAILED{RESET} {} — {err}", position.mint),
            )
            .await;

            if let Some(pos) = app.positions.write().await.get_mut(&position.mint) {
                pos.selling = false;
            }
        }
    }
}

async fn execute_trade(
    app: &App,
    action: &str,
    mint: &str,
    amount: f64,
    denominated_in_sol: bool,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let hot = *app.hot.read().await;

    let pool = if action == "sell" {
        if app.migrated.lock().await.contains(mint) {
            "pump-amm"
        } else {
            "pump"
        }
    } else {
        "pump"
    };

    let body = json!({
        "publicKey": app.keypair.pubkey().to_string(),
        "action": action,
        "mint": mint,
        "amount": if action == "sell" {
            json!("100%")
        } else {
            json!(amount)
        },
        "denominatedInSol": if action == "sell" {
            "false"
        } else if denominated_in_sol {
            "true"
        } else {
            "false"
        },
        "slippage": hot.slippage_pct,
        "priorityFee": hot.priority_fee_sol,
        "pool": pool
    });

    let response = app
        .http
        .post(PUMPPORTAL_LOCAL_TX)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();

        return Err(
            format!("PumpPortal HTTP {}: {}", status, text).into()
        );
    }

    let bytes = response.bytes().await?;

    if bytes.is_empty() {
        return Err("PumpPortal returned an empty transaction".into());
    }

    let unsigned: VersionedTransaction =
        bincode::deserialize(&bytes)?;

    let signed = VersionedTransaction::try_new(
        unsigned.message,
        &[app.keypair.as_ref()],
    )?;

    let encoded = base64::engine::general_purpose::STANDARD
        .encode(bincode::serialize(&signed)?);

    if hot.mev_protection {
        send_jito(app, &signed).await
    } else {
        send_rpc(app, encoded).await
    }
}

async fn send_rpc(
    app: &App,
    encoded: String,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let payload: Value = app
        .http
        .post(&app.config.rpc_url)
        .json(&json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"sendTransaction",
            "params":[
                encoded,
                {
                    "encoding":"base64",
                    "skipPreflight":true,
                    "maxRetries":0,
                    "preflightCommitment":"processed"
                }
            ]
        }))
        .send()
        .await?
        .json()
        .await?;

    if let Some(error) = payload.get("error") {
        return Err(format!("RPC sendTransaction error: {}", error).into());
    }

    payload
        .get("result")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("RPC returned no signature: {}", payload).into())
}

async fn send_jito(
    app: &App,
    signed: &VersionedTransaction,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let tx = bs58::encode(bincode::serialize(signed)?).into_string();

    let payload: Value = app
        .http
        .post(JITO_BUNDLE_URL)
        .json(&json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"sendBundle",
            "params":[[tx]]
        }))
        .send()
        .await?
        .json()
        .await?;

    if let Some(error) = payload.get("error") {
        return Err(format!("Jito error: {}", error).into());
    }

    signed
        .signatures
        .first()
        .map(ToString::to_string)
        .ok_or_else(|| "Signed transaction has no signature".into())
}

async fn subscribe_token(app: &App, mint: &str) {
    let sender = app.ws_tx.lock().await.clone();

    if let Some(sender) = sender {
        let _ = sender.send(Message::Text(
            json!({
                "method":"subscribeTokenTrade",
                "keys":[mint]
            })
            .to_string()
            .into(),
        ));
    }
}

async fn unsubscribe_token(app: &App, mint: &str) {
    let sender = app.ws_tx.lock().await.clone();

    if let Some(sender) = sender {
        let _ = sender.send(Message::Text(
            json!({
                "method":"unsubscribeTokenTrade",
                "keys":[mint]
            })
            .to_string()
            .into(),
        ));
    }
}

async fn command_loop(app: App) {
    log_line(&app, "[CTRL] on | off | status | quit").await;

    let stdin = BufReader::new(io::stdin());
    let mut lines = stdin.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        match line.trim().to_ascii_lowercase().as_str() {
            "on" | "buy on" => {
                app.config.auto_buy.store(true, Ordering::Relaxed);
                log_line(&app, "[CTRL] AUTO BUY ON").await;
            }

            "off" | "buy off" | "stop" => {
                app.config.auto_buy.store(false, Ordering::Relaxed);
                log_line(&app, "[CTRL] AUTO BUY OFF").await;
            }

            "status" => {
                let positions = app.positions.read().await;
                let hot = *app.hot.read().await;

                log_line(
                    &app,
                    &format!(
                        "[STATUS] auto_buy={} positions={} bought={} migrations={} trade_amount={:.4} SOL slippage={:.1}%",
                        on_off(app.config.auto_buy.load(Ordering::Relaxed)),
                        positions.len(),
                        app.bought_count.load(Ordering::Relaxed),
                        app.migration_count.load(Ordering::Relaxed),
                        hot.trade_amount_sol,
                        hot.slippage_pct,
                    ),
                )
                .await;
            }

            "quit" | "exit" => {
                app.config.auto_buy.store(false, Ordering::Relaxed);
                app.config.shutdown.store(true, Ordering::Relaxed);
                log_line(&app, "[CTRL] shutdown").await;
                break;
            }

            _ => {
                log_line(&app, "[CTRL] on | off | status | quit").await;
            }
        }
    }
}

fn parse_new_token(v: &Value) -> Option<NewToken> {
    if v.get("txType").and_then(Value::as_str) != Some("create") {
        return None;
    }

    let mint = string_field(v, "mint")?;
    let name = string_field(v, "name").unwrap_or_else(|| "Unknown".to_string());
    let symbol = string_field(v, "symbol").unwrap_or_else(|| "?".to_string());

    let source_timestamp_ms = v
        .get("timestamp")
        .or_else(|| v.get("createdTimestamp"))
        .or_else(|| v.get("created_timestamp"))
        .and_then(|value| value.as_u64())
        .map(|ts| {
            if ts < 1_000_000_000_000 {
                ts.saturating_mul(1_000)
            } else {
                ts
            }
        });

    Some(NewToken {
        mint,
        name,
        symbol,
        received_at: Instant::now(),
        source_timestamp_ms,
    })
}

fn parse_trade_event(v: &Value) -> Option<TradeEvent> {
    Some(TradeEvent {
        mint: string_field(v, "mint")?,
        tx_type: string_field(v, "txType").unwrap_or_default(),
        trader: string_field(v, "traderPublicKey")
            .or_else(|| string_field(v, "trader"))
            .unwrap_or_default(),
        sol_amount: number_field(v, "solAmount")?,
        token_amount: number_field(v, "tokenAmount")?,
    })
}

fn string_field(v: &Value, key: &str) -> Option<String> {
    v.get(key)?.as_str().map(str::to_owned)
}

fn number_field(v: &Value, key: &str) -> Option<f64> {
    let value = v.get(key)?;

    value
        .as_f64()
        .or_else(|| value.as_u64().map(|n| n as f64))
        .or_else(|| value.as_i64().map(|n| n as f64))
        .or_else(|| value.as_str()?.parse::<f64>().ok())
}

fn is_fresh(token: &NewToken, max_age_ms: u64) -> bool {
    if let Some(timestamp) = token.source_timestamp_ms {
        let now = unix_ms();

        if now >= timestamp {
            return now - timestamp <= max_age_ms;
        }
    }

    // If PumpPortal does not provide a usable timestamp,
    // the event is treated as fresh when it reaches this process.
    token.received_at.elapsed().as_millis() as u64 <= max_age_ms
}

fn load_keypair(
    private_key: &str,
) -> Result<Keypair, Box<dyn Error + Send + Sync>> {
    let bytes = bs58::decode(private_key.trim())
        .into_vec()
        .map_err(|e| format!("Invalid base58 PRIVATE_KEY: {e}"))?;

    match bytes.len() {
        32 => Keypair::from_seed(&bytes)
            .map_err(|e| format!("Invalid 32-byte private key seed: {e}").into()),

        64 => Keypair::from_bytes(&bytes)
            .map_err(|e| format!("Invalid 64-byte private key: {e}").into()),

        n => Err(
            format!(
                "PRIVATE_KEY decoded to {} bytes, expected 32 or 64",
                n
            )
            .into(),
        ),
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

// ---- live .env reload ----

/// Reads a dotenv-style file straight off disk into a map, without touching
/// process env vars. Used for polling so we can detect changes at runtime.
fn read_env_file(path: &Path) -> HashMap<String, String> {
    let mut map = HashMap::new();

    let Ok(contents) = std::fs::read_to_string(path) else {
        return map;
    };

    for line in contents.lines() {
        let line = line.trim();

        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let Some((key, value)) = line.split_once('=') else {
            continue;
        };

        let key = key.trim().to_string();
        let mut value = value.trim().to_string();

        if value.len() >= 2 {
            let bytes = value.as_bytes();
            let quoted = (bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"')
                || (bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\'');

            if quoted {
                value = value[1..value.len() - 1].to_string();
            }
        }

        map.insert(key, value);
    }

    map
}

fn map_bool(map: &HashMap<String, String>, key: &str, default: bool) -> bool {
    map.get(key).and_then(|v| v.parse::<bool>().ok()).unwrap_or(default)
}

fn map_f64(map: &HashMap<String, String>, key: &str, default: f64) -> f64 {
    map.get(key).and_then(|v| v.parse::<f64>().ok()).unwrap_or(default)
}

fn map_u64(map: &HashMap<String, String>, key: &str, default: u64) -> u64 {
    map.get(key).and_then(|v| v.parse::<u64>().ok()).unwrap_or(default)
}

fn hot_settings_from_map(map: &HashMap<String, String>) -> (bool, HotSettings) {
    let auto_buy = map_bool(map, "AUTO_BUY_NEW_TOKENS", AUTO_BUY_DEFAULT);

    let settings = HotSettings {
        trade_amount_sol: map_f64(map, "TRADE_AMOUNT_SOL", TRADE_AMOUNT_DEFAULT),
        max_token_age_ms: map_u64(map, "MAX_TOKEN_AGE_MS", MAX_TOKEN_AGE_DEFAULT),
        auto_sell_profit_pct: map_f64(map, "AUTO_SELL_PROFIT_PCT", AUTO_SELL_PROFIT_DEFAULT),
        slippage_pct: map_f64(map, "SLIPPAGE_PCT", SLIPPAGE_DEFAULT),
        priority_fee_sol: map_f64(map, "PRIORITY_FEE_SOL", PRIORITY_FEE_DEFAULT),
        mev_protection: map_bool(map, "MEV_PROTECTION", MEV_PROTECTION_DEFAULT),
        pump_tokens_only: map_bool(map, "PUMP_TOKENS_ONLY", PUMP_ONLY_DEFAULT),
        min_sol_reserve: map_f64(map, "MIN_SOL_RESERVE", MIN_SOL_RESERVE_DEFAULT),
    };

    (auto_buy, settings)
}

/// Polls the .env file for changes. Only applies values that actually
/// changed in the file itself, so a manual on/off via the stdin console
/// isn't stomped on every poll tick — only when you edit and save .env.
async fn settings_watcher(app: App) {
    let path = app.env_path.clone();

    if !path.exists() {
        log_line(
            &app,
            &format!(
                "[SETTINGS] no .env file found at {} — live reload disabled, restart to apply changes",
                path.display()
            ),
        )
        .await;
        return;
    }

    let mut last_mtime = std::fs::metadata(path.as_path()).and_then(|m| m.modified()).ok();
    let mut last_seen = hot_settings_from_map(&read_env_file(path.as_path()));

    loop {
        sleep(Duration::from_millis(750)).await;

        if app.config.shutdown.load(Ordering::Relaxed) {
            return;
        }

        let Ok(mtime) = std::fs::metadata(path.as_path()).and_then(|m| m.modified()) else {
            continue;
        };

        if Some(mtime) == last_mtime {
            continue;
        }
        last_mtime = Some(mtime);

        // Small grace period in case the editor is still mid-write.
        sleep(Duration::from_millis(50)).await;

        let map = read_env_file(path.as_path());
        let (file_auto_buy, file_settings) = hot_settings_from_map(&map);

        if (file_auto_buy, file_settings) == last_seen {
            continue;
        }
        last_seen = (file_auto_buy, file_settings);

        let current_auto_buy = app.config.auto_buy.load(Ordering::Relaxed);
        if file_auto_buy != current_auto_buy {
            app.config.auto_buy.store(file_auto_buy, Ordering::Relaxed);
            log_line(
                &app,
                &format!(
                    "[SETTINGS] auto_buy: {} -> {} (.env changed)",
                    on_off(current_auto_buy),
                    on_off(file_auto_buy)
                ),
            )
            .await;
        }

        let current_settings = *app.hot.read().await;
        if file_settings != current_settings {
            *app.hot.write().await = file_settings;
            log_line(&app, "[SETTINGS] trade parameters reloaded from .env").await;
        }
    }
}

// ---- wallet balance polling ----

async fn balance_watcher(app: App) {
    loop {
        if app.config.shutdown.load(Ordering::Relaxed) {
            return;
        }

        match fetch_sol_balance(&app).await {
            Ok(balance) => {
                *app.sol_balance.write().await = Some(balance);
                refresh_status(&app).await;
            }
            Err(err) => {
                eprintln!("[BALANCE] {err}");
            }
        }

        sleep(Duration::from_secs(15)).await;
    }
}

async fn fetch_sol_balance(app: &App) -> Result<f64, Box<dyn Error + Send + Sync>> {
    let payload: Value = app
        .http
        .post(&app.config.rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [app.keypair.pubkey().to_string()]
        }))
        .send()
        .await?
        .json()
        .await?;

    let lamports = payload
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("getBalance returned no value: {payload}"))?;

    Ok(lamports as f64 / 1_000_000_000.0)
}

// ---- display helpers ----

fn on_off(value: bool) -> &'static str {
    if value {
        "ON"
    } else {
        "OFF"
    }
}

/// OSC 8 terminal hyperlink escape sequence. Supported by most modern
/// terminals (GNOME Terminal, Konsole, kitty, Alacritty, iTerm2, Windows
/// Terminal). In terminals without support, the label text still shows
/// and can be copied/opened manually.
fn hyperlink(url: &str, label: &str) -> String {
    format!("\x1b]8;;{url}\x1b\\{label}\x1b]8;;\x1b\\")
}

fn token_card(
    label: &str,
    color: &str,
    name: &str,
    symbol: &str,
    mint: &str,
) -> String {
    let url = format!("https://pump.fun/coin/{mint}");
    let link = hyperlink(&url, &url);

    let title = if label.eq_ignore_ascii_case("MIGRATION") {
        MIGRATION_TITLE
    } else {
        PLATFORM_TITLE
    };

    let name_text = format!("Name   {} ({})", name, symbol);
    let mint_text = format!("Mint   {}", mint);
    let link_text = format!("Link   {}", url);

    let content_width = TOKEN_BOX_WIDTH
        .max(name_text.chars().count())
        .max(mint_text.chars().count())
        .max(link_text.chars().count());

    let horizontal = "─".repeat(content_width + 2);

    let pad = |length: usize| -> String {
        " ".repeat(content_width.saturating_sub(length))
    };

    [
        format!("{color}┌{horizontal}┐{RESET}"),

        format!(
            "{color}│{RESET} {BOLD}{title}{RESET} {DIM}· {label}{RESET}{} {color}│{RESET}",
            pad(title.chars().count() + 3 + label.chars().count() + 1)
        ),

        format!(
            "{color}│{RESET}{} {color}│{RESET}",
            " ".repeat(content_width + 1)
        ),

        format!(
            "{color}│{RESET}  Name   {BOLD}{name}{RESET} {DIM}({symbol}){RESET}{} {color}│{RESET}",
            pad(name_text.chars().count())
        ),

        format!(
            "{color}│{RESET}  Mint   {DIM}{mint}{RESET}{} {color}│{RESET}",
            pad(mint_text.chars().count())
        ),

        format!(
            "{color}│{RESET}  Link   {CYAN}{UNDERLINE}{link}{RESET}{} {color}│{RESET}",
            pad(link_text.chars().count())
        ),

        format!("{color}└{horizontal}┘{RESET}"),
    ]
    .join("\n")
}


async fn print_status_line_locked(app: &App) {
    let auto_buy = app.config.auto_buy.load(Ordering::Relaxed);
    let bought = app.bought_count.load(Ordering::Relaxed);
    let migrations = app.migration_count.load(Ordering::Relaxed);
    let balance = *app.sol_balance.read().await;

    let balance_str = match balance {
        Some(b) => format!("{b:.4} SOL"),
        None => "…".to_string(),
    };

    let auto_buy_str = if auto_buy {
        format!("{GREEN}{BOLD}AUTO-BUY ON{RESET}")
    } else {
        format!("{RED}{BOLD}AUTO-BUY OFF{RESET}")
    };

    print!(
        "\r\x1b[2K{DIM}Wallet{RESET} {balance_str}  │  {auto_buy_str}  │  {DIM}Migrations{RESET} {migrations}  {DIM}Bought{RESET} {bought}  │  {GREEN}●{RESET} LIVE"
    );

    let _ = std::io::Write::flush(&mut std::io::stdout());
}

/// Prints a log line above the persistent status line, then redraws the
/// status line underneath it. All stdout writes go through the print_lock
/// so concurrent tasks (buy/sell handlers, settings watcher, balance
/// watcher) don't interleave mid-line.
async fn log_line(app: &App, text: &str) {
    let _guard = app.print_lock.lock().await;
    print!("\r\x1b[2K");
    println!("{text}");
    print_status_line_locked(app).await;
}

/// Redraws just the status line in place (no new log entry).
async fn refresh_status(app: &App) {
    let _guard = app.print_lock.lock().await;
    print_status_line_locked(app).await;
}

async fn print_startup(
    app: &App,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let pubkey = app.keypair.pubkey();
    let hot = *app.hot.read().await;

    println!();
    println!("{CYAN}{PUMP_ASCII}{RESET}");
    println!();

    println!("{GREEN}{BOLD}========== PUMP.FUN SNIPER =========={RESET}");
    println!("Wallet: {}", pubkey);
    println!(
        "Auto buy: {}",
        on_off(app.config.auto_buy.load(Ordering::Relaxed))
    );
    println!("Trade: {:.4} SOL", hot.trade_amount_sol);
    println!("Max token age: {} ms", hot.max_token_age_ms);
    println!("Auto sell: +{:.0}%", hot.auto_sell_profit_pct);
    println!("Slippage: {:.1}%", hot.slippage_pct);
    println!("Priority fee: {:.9} SOL", hot.priority_fee_sol);
    println!("MEV protection: {}", hot.mev_protection);
    println!("Pump.fun only: {}", hot.pump_tokens_only);
    println!("Minimum reserve: {:.4} SOL", hot.min_sol_reserve);
    println!("Live .env reload: watching {}", app.env_path.display());
    println!("=====================================");

    println!();
    println!("{DIM}{MONITOR_LINE}{RESET}");
    println!("{GREEN}{BOLD}LIVE TOKEN MONITOR{RESET}");
    println!("{DIM}{MONITOR_LINE}{RESET}");
    println!();

    refresh_status(app).await;

    Ok(())
}