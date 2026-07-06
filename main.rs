use crate::state::{AppState, Position, TradeRecord, WatchedToken};
use serde::{Deserialize, Serialize};
use std::path::Path;

const STORE_PATH: &str = "pumpsniper_data.json";

#[derive(Debug, Serialize, Deserialize, Default)]
struct PersistedData {
    positions: Vec<Position>,
    history: Vec<TradeRecord>,
    watched: Vec<WatchedToken>,
    total_spent_sol: f64,
    daily_realized_pnl_sol: f64,
    day_start_timestamp: i64,
}

pub fn load_into(state: &crate::state::SharedState) -> anyhow::Result<()> {
    if !Path::new(STORE_PATH).exists() {
        return Ok(());
    }
    let raw = std::fs::read_to_string(STORE_PATH)?;
    let data: PersistedData = serde_json::from_str(&raw)?;
    let mut s = state.lock().unwrap();
    s.positions = data.positions;
    s.history = data.history;
    s.watched = data.watched;
    s.total_spent_sol = data.total_spent_sol;

    // Only carry the realized P&L forward if it's still "today"; otherwise
    // start today's counter fresh (item 11: daily profit/loss).
    let now = chrono::Utc::now();
    let stored_day = chrono::DateTime::from_timestamp(data.day_start_timestamp, 0)
        .unwrap_or(now)
        .date_naive();
    if stored_day == now.date_naive() {
        s.daily_realized_pnl_sol = data.daily_realized_pnl_sol;
        s.day_start_timestamp = data.day_start_timestamp;
    } else {
        s.daily_realized_pnl_sol = 0.0;
        s.day_start_timestamp = now.timestamp();
    }
    Ok(())
}

pub fn save_from(state: &crate::state::SharedState) -> anyhow::Result<()> {
    let s = state.lock().unwrap();
    let data = PersistedData {
        positions: s.positions.clone(),
        history: s.history.clone(),
        watched: s.watched.clone(),
        total_spent_sol: s.total_spent_sol,
        daily_realized_pnl_sol: s.daily_realized_pnl_sol,
        day_start_timestamp: s.day_start_timestamp,
    };
    drop(s);
    let raw = serde_json::to_string_pretty(&data)?;
    std::fs::write(STORE_PATH, raw)?;
    Ok(())
}
