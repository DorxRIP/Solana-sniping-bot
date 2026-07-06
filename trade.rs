use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub mint: String,
    pub symbol: String,
    pub name: String,
    pub entry_price_sol: f64,
    pub token_amount: f64,
    pub sol_spent: f64,
    pub opened_at: i64,
    pub current_price_sol: f64,
    pub dev_holder_pct_at_entry: f64,
}

impl Position {
    pub fn unrealized_pct(&self) -> f64 {
        if self.entry_price_sol <= 0.0 {
            return 0.0;
        }
        ((self.current_price_sol - self.entry_price_sol) / self.entry_price_sol) * 100.0
    }

    pub fn age_seconds(&self) -> i64 {
        chrono::Utc::now().timestamp() - self.opened_at
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub mint: String,
    pub symbol: String,
    pub side: String, // "buy" | "sell"
    pub sol_amount: f64,
    pub token_amount: f64,
    pub price_sol: f64,
    /// Network fee + priority fee + pump.fun platform fee, all in SOL.
    pub fees_sol: f64,
    pub tx_signature: Option<String>,
    pub timestamp: i64,
    /// "manual" | "take_profit" | "stop_loss" | "max_hold" | "dry_run"
    pub reason: String,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WatchedToken {
    pub mint: String,
    pub symbol: String,
    pub name: String,
    pub detected_at: i64,
    pub dev_holder_pct: Option<f64>,
    /// If Some, explains why the bot did NOT buy this one.
    pub skipped_reason: Option<String>,
    pub bought: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppState {
    pub running: bool,
    pub dry_run: bool,
    pub sol_balance: f64,
    pub gbp_per_sol: f64,
    pub total_spent_sol: f64,
    pub daily_realized_pnl_sol: f64,
    pub day_start_timestamp: i64,
    pub positions: Vec<Position>,
    pub history: Vec<TradeRecord>,
    /// Most recent detections first, capped at a few hundred entries.
    pub watched: Vec<WatchedToken>,
    pub log: Vec<String>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            running: false,
            dry_run: true,
            sol_balance: 0.0,
            gbp_per_sol: 0.0,
            total_spent_sol: 0.0,
            daily_realized_pnl_sol: 0.0,
            day_start_timestamp: chrono::Utc::now().timestamp(),
            positions: Vec::new(),
            history: Vec::new(),
            watched: Vec::new(),
            log: Vec::new(),
        }
    }
}

impl AppState {
    pub fn push_log(&mut self, msg: impl Into<String>) {
        let msg = msg.into();
        let stamped = format!("[{}] {}", chrono::Local::now().format("%H:%M:%S"), msg);
        self.log.push(stamped);
        if self.log.len() > 500 {
            let excess = self.log.len() - 500;
            self.log.drain(0..excess);
        }
    }

    pub fn push_watched(&mut self, token: WatchedToken) {
        self.watched.insert(0, token);
        if self.watched.len() > 300 {
            self.watched.truncate(300);
        }
    }
}

pub type SharedState = Arc<Mutex<AppState>>;

pub fn new_shared_state(dry_run: bool) -> SharedState {
    let mut s = AppState::default();
    s.dry_run = dry_run;
    Arc::new(Mutex::new(s))
}
