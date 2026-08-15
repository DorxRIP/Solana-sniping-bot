use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::error::Error;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

const WS_URL: &str = "wss://pumpportal.fun/api/data";
const RECONNECT_DELAY: Duration = Duration::from_secs(4);

#[tokio::main]
async fn main() {
    loop {
        match connect_and_listen().await {
            Ok(_) => {
                println!("WebSocket session ended. Reconnecting in {}s...", RECONNECT_DELAY.as_secs());
            }
            Err(err) => {
                eprintln!("WebSocket error: {}. Reconnecting in {}s...", err, RECONNECT_DELAY.as_secs());
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn connect_and_listen() -> Result<(), Box<dyn Error>> {
    println!("Connecting to {}...", WS_URL);
    let url = Url::parse(WS_URL)?;
    let (mut ws_stream, _) = connect_async(url).await?;

    println!("Connected. Subscribing to new tokens...");
    ws_stream
        .send(Message::Text(r#"{"method":"SubscribeNewToken"}"#.into()))
        .await?;

    loop {
        match ws_stream.next().await {
            Some(Ok(Message::Text(text))) => {
                if let Err(err) = handle_message(&text) {
                    eprintln!("Failed to parse message: {} | raw: {}", err, text);
                }
            }
            Some(Ok(Message::Binary(bytes))) => {
                let text = String::from_utf8_lossy(&bytes);
                if let Err(err) = handle_message(&text) {
                    eprintln!("Failed to parse binary message: {} | raw: {}", err, text);
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

fn handle_message(raw: &str) -> Result<(), Box<dyn Error>> {
    let value: Value = serde_json::from_str(raw)?;
    let record = find_token_record(&value);

    let Some(record) = record else {
        return Ok(());
    };

    let mint = get_string(&record, &["mint", "tokenMint", "token_mint"]).unwrap_or_else(|| "N/A".to_string());
    let name = get_string(&record, &["name"]).unwrap_or_else(|| "N/A".to_string());
    let symbol = get_string(&record, &["symbol"]).unwrap_or_else(|| "N/A".to_string());
    let creator = get_string(&record, &["creator", "creatorAddress", "creator_address"]).unwrap_or_else(|| "N/A".to_string());
    let market_cap = get_string(&record, &["marketCap", "market_cap", "mc", "marketcap"]).unwrap_or_else(|| "N/A".to_string());

    if mint == "N/A" {
        return Ok(());
    }

    println!(
        "Token: {} | Symbol: {} | Mint: {} | Creator: {} | Market Cap: {}",
        name, symbol, mint, creator, market_cap
    );

    Ok(())
}

fn find_token_record(value: &Value) -> Option<Value> {
    if value.is_object() {
        if value.get("mint").is_some() {
            return Some(value.clone());
        }

        if let Some(data) = value.get("data") {
            if data.get("mint").is_some() {
                return Some(data.clone());
            }
        }
    }

    if let Some(obj) = value.as_object() {
        for (_, child) in obj {
            if let Some(found) = find_token_record(child) {
                return Some(found);
            }
        }
    }

    if let Some(arr) = value.as_array() {
        for child in arr {
            if let Some(found) = find_token_record(child) {
                return Some(found);
            }
        }
    }

    None
}

fn get_string(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(s) = v.as_str() {
                return Some(s.to_string());
            }
            if let Some(num) = v.as_f64() {
                return Some(num.to_string());
            }
            if let Some(num) = v.as_i64() {
                return Some(num.to_string());
            }
            if let Some(num) = v.as_u64() {
                return Some(num.to_string());
            }
        }
    }
    None
}
