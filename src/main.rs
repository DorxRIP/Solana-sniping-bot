use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

const WS_URL: &str = "wss://pumpportal.fun/api/data";
const RECONNECT_DELAY: Duration = Duration::from_secs(4);

static TOKEN_COUNT: AtomicUsize = AtomicUsize::new(1);

const RESET: &str = "\x1b[0m";
const GREEN: &str = "\x1b[32m";
const CYAN: &str = "\x1b[36m";
const YELLOW: &str = "\x1b[33m";
const BLUE: &str = "\x1b[34m";
const WHITE: &str = "\x1b[37m";

#[tokio::main]
async fn main() {
    loop {
        match connect_and_listen().await {
            Ok(_) => {
                println!("{}WebSocket session ended. Reconnecting in {}s...{}", YELLOW, RECONNECT_DELAY.as_secs(), RESET);
            }
            Err(err) => {
                eprintln!("{}WebSocket error: {}. Reconnecting in {}s...{}", YELLOW, err, RECONNECT_DELAY.as_secs(), RESET);
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn connect_and_listen() -> Result<(), Box<dyn Error>> {
    println!("\n{}PUMP.FUN TOKEN SCANNER{}", YELLOW, RESET);
    println!("{}Real-time new token detection via PumpPortal{}\n", CYAN, RESET);

    println!("[{}] {}Connecting to PumpPortal WebSocket...{}", timestamp(), YELLOW, RESET);
    let (mut ws_stream, _) = connect_async(WS_URL).await?;

    println!("[{}] {}Connected{}", timestamp(), GREEN, RESET);
    println!("[{}] {}Subscribing to new token events...{}", timestamp(), YELLOW, RESET);

    ws_stream
        .send(Message::Text(r#"{"method":"subscribeNewToken"}"#.into()))
        .await?;

    println!("[{}] {}Subscribed — waiting for new tokens...{}", timestamp(), GREEN, RESET);

    loop {
        match ws_stream.next().await {
            Some(Ok(Message::Text(text))) => {
                if let Some(token) = parse_token_message(&text) {
                    print_token(token);
                } else if text.contains("Successfully subscribed") {
                    println!("[{}] {}Subscribed — waiting for new tokens...{}", timestamp(), GREEN, RESET);
                } else if text.contains("Invalid") || text.contains("error") || text.contains("Error") {
                    eprintln!("[{}] {}{}{}", timestamp(), YELLOW, text, RESET);
                }
            }
            Some(Ok(Message::Binary(bytes))) => {
                let text = String::from_utf8_lossy(&bytes);
                if let Some(token) = parse_token_message(&text) {
                    print_token(token);
                }
            }
            Some(Ok(Message::Ping(_))) => {
                ws_stream.send(Message::Pong(vec![].into())).await?;
            }
            Some(Ok(Message::Close(_))) | None => {
                return Err("WebSocket closed by server".into());
            }
            Some(Ok(_)) => {}
            Some(Err(err)) => return Err(Box::new(err)),
        }
    }
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
    let market_cap = get_f64(&value, &["marketCapSol", "market_cap", "marketCap"]).map(format_market_cap).unwrap_or_else(|| "N/A".to_string());
    let initial_buy = get_f64(&value, &["initialBuy"]).map(format_initial_buy).unwrap_or_else(|| "N/A".to_string());
    let bonding_sol = get_f64(&value, &["solAmount"]).map(|v| format!("{:.4} SOL", v)).unwrap_or_else(|| "N/A".to_string());
    let metadata = get_string(&value, &["uri", "metadata"]).unwrap_or_else(|| "N/A".to_string());
    let pump_url = format!("https://pump.fun/coin/{}", mint);
    let solscan_url = format!("https://solscan.io/token/{}", mint);

    Some(Token {
        mint,
        name,
        symbol,
        creator,
        bonding_curve,
        market_cap,
        initial_buy,
        bonding_sol,
        metadata,
        pump_url,
        solscan_url,
    })
}

struct Token {
    mint: String,
    name: String,
    symbol: String,
    creator: String,
    bonding_curve: String,
    market_cap: String,
    initial_buy: String,
    bonding_sol: String,
    metadata: String,
    pump_url: String,
    solscan_url: String,
}

fn print_token(token: Token) {
    let id = TOKEN_COUNT.fetch_add(1, Ordering::Relaxed);

    println!("\n{}● {}NEW TOKEN #{}{}", GREEN, YELLOW, id, RESET);
    println!("{}Name:{} {}{}", CYAN, RESET, WHITE, token.name);
    println!("{}Symbol:{} {}{}", CYAN, RESET, WHITE, token.symbol);
    println!("{}Mint:{} {}{}", CYAN, RESET, BLUE, token.mint);
    println!("{}Creator:{} {}{}", CYAN, RESET, WHITE, token.creator);
    println!("{}Bonding Curve:{} {}{}", CYAN, RESET, BLUE, token.bonding_curve);
    println!("{}Market Cap:{} {}{}", CYAN, RESET, YELLOW, token.market_cap);
    println!("{}Initial Buy:{} {}{}", CYAN, RESET, WHITE, token.initial_buy);
    println!("{}Bonding SOL:{} {}{}", CYAN, RESET, WHITE, token.bonding_sol);
    println!("{}Metadata:{} {}{}", CYAN, RESET, BLUE, token.metadata);
    println!("{}Solscan:{} {}{}", CYAN, RESET, BLUE, token.solscan_url);
    println!("{}Pump.fun:{} {}{}", CYAN, RESET, BLUE, token.pump_url);
    println!("{}Time:{} {}{}\n", CYAN, RESET, WHITE, timestamp());
    println!("{}----------------------------------------{}", YELLOW, RESET);
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
