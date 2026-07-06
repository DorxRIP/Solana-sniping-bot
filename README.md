use crate::config::Config;
use crate::state::SharedState;
use eframe::egui;
use std::sync::{Arc, Mutex};

#[derive(PartialEq)]
enum Tab {
    Dashboard,
    History,
    ActiveCoins,
    Settings,
    Log,
}

/// Local, editable copies of config fields for the Settings tab. Kept as
/// strings so the user can type freely; parsed + validated on Save.
struct SettingsDraft {
    rpc_http_url: String,
    rpc_ws_url: String,
    dry_run: bool,
    buy_amount_sol: String,
    max_total_sol_cap: String,
    slippage_pct: String,
    take_profit_pct: String,
    stop_loss_pct: String,
    max_dev_holder_pct: String,
    max_hold_enabled: bool,
    max_hold_seconds: String,
}

impl SettingsDraft {
    fn from_config(cfg: &Config) -> Self {
        Self {
            rpc_http_url: cfg.rpc_http_url.clone(),
            rpc_ws_url: cfg.rpc_ws_url.clone(),
            dry_run: cfg.dry_run,
            buy_amount_sol: cfg.buy_amount_sol.to_string(),
            max_total_sol_cap: cfg.max_total_sol_cap.to_string(),
            slippage_pct: (cfg.slippage_bps as f64 / 100.0).to_string(),
            take_profit_pct: cfg.take_profit_pct.to_string(),
            stop_loss_pct: cfg.stop_loss_pct.to_string(),
            max_dev_holder_pct: cfg.max_dev_holder_pct.to_string(),
            max_hold_enabled: cfg.max_hold_enabled,
            max_hold_seconds: cfg.max_hold_seconds.to_string(),
        }
    }
}

pub struct PumpSniperApp {
    state: SharedState,
    cfg: Arc<Mutex<Config>>,
    config_path: String,
    tab: Tab,
    draft: SettingsDraft,
    settings_message: Option<String>,
}

impl PumpSniperApp {
    pub fn new(state: SharedState, cfg: Arc<Mutex<Config>>, config_path: String) -> Self {
        let draft = SettingsDraft::from_config(&cfg.lock().unwrap());
        Self {
            state,
            cfg,
            config_path,
            tab: Tab::Dashboard,
            draft,
            settings_message: None,
        }
    }
}

impl eframe::App for PumpSniperApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Keep repainting a couple times a second so background-thread
        // updates (prices, fills, log lines) show up without needing input.
        ctx.request_repaint_after(std::time::Duration::from_millis(500));

        self.header(ctx);
        self.tab_bar(ctx);

        egui::CentralPanel::default().show(ctx, |ui| match self.tab {
            Tab::Dashboard => self.dashboard(ui),
            Tab::History => self.history(ui),
            Tab::ActiveCoins => self.active_coins(ui),
            Tab::Settings => self.settings(ui),
            Tab::Log => self.log(ui),
        });
    }
}

impl PumpSniperApp {
    fn header(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("header").show(ctx, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                let mut s = self.state.lock().unwrap();

                ui.heading("Pump Sniper");
                ui.separator();

                if s.dry_run {
                    ui.colored_label(egui::Color32::YELLOW, "DRY RUN");
                    ui.separator();
                }

                ui.label(format!("{:.4} SOL", s.sol_balance));
                if s.gbp_per_sol > 0.0 {
                    ui.label(format!("(£{:.2})", s.sol_balance * s.gbp_per_sol));
                }
                ui.separator();

                let pnl = s.daily_realized_pnl_sol;
                let pnl_color = if pnl >= 0.0 {
                    egui::Color32::from_rgb(80, 200, 120)
                } else {
                    egui::Color32::from_rgb(220, 90, 90)
                };
                ui.label("Today's P&L:");
                ui.colored_label(pnl_color, format!("{pnl:+.4} SOL"));
                ui.separator();

                ui.label(format!(
                    "Spent: {:.4} / {:.4} SOL cap",
                    s.total_spent_sol,
                    self.cfg.lock().unwrap().max_total_sol_cap
                ));

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if s.running {
                        if ui.button("Stop").clicked() {
                            s.running = false;
                            s.push_log("Bot stopped.".to_string());
                        }
                        ui.colored_label(egui::Color32::from_rgb(80, 200, 120), "RUNNING");
                    } else {
                        if ui.button("Start").clicked() {
                            s.running = true;
                            s.push_log("Bot started.".to_string());
                        }
                        ui.colored_label(egui::Color32::GRAY, "STOPPED");
                    }
                });
            });
            ui.add_space(4.0);
        });
    }

    fn tab_bar(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("tabs").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.tab, Tab::Dashboard, "Current Trades");
                ui.selectable_value(&mut self.tab, Tab::History, "Trade History");
                ui.selectable_value(&mut self.tab, Tab::ActiveCoins, "Active Coins");
                ui.selectable_value(&mut self.tab, Tab::Settings, "Settings");
                ui.selectable_value(&mut self.tab, Tab::Log, "Log");
            });
        });
    }

    fn dashboard(&mut self, ui: &mut egui::Ui) {
        let s = self.state.lock().unwrap();
        ui.label(format!("{} open position(s)", s.positions.len()));
        ui.separator();

        egui::ScrollArea::vertical().show(ui, |ui| {
            egui_extras::TableBuilder::new(ui)
                .striped(true)
                .column(egui_extras::Column::auto().at_least(80.0))
                .column(egui_extras::Column::auto().at_least(90.0))
                .column(egui_extras::Column::auto().at_least(90.0))
                .column(egui_extras::Column::auto().at_least(70.0))
                .column(egui_extras::Column::auto().at_least(90.0))
                .column(egui_extras::Column::remainder())
                .header(20.0, |mut header| {
                    header.col(|ui| { ui.strong("Symbol"); });
                    header.col(|ui| { ui.strong("Entry (SOL)"); });
                    header.col(|ui| { ui.strong("Current (SOL)"); });
                    header.col(|ui| { ui.strong("P&L %"); });
                    header.col(|ui| { ui.strong("SOL spent"); });
                    header.col(|ui| { ui.strong("Age"); });
                })
                .body(|mut body| {
                    for pos in s.positions.iter() {
                        body.row(20.0, |mut row| {
                            row.col(|ui| { ui.label(&pos.symbol); });
                            row.col(|ui| { ui.label(format!("{:.10}", pos.entry_price_sol)); });
                            row.col(|ui| { ui.label(format!("{:.10}", pos.current_price_sol)); });
                            row.col(|ui| {
                                let pct = pos.unrealized_pct();
                                let color = if pct >= 0.0 {
                                    egui::Color32::from_rgb(80, 200, 120)
                                } else {
                                    egui::Color32::from_rgb(220, 90, 90)
                                };
                                ui.colored_label(color, format!("{pct:+.1}%"));
                            });
                            row.col(|ui| { ui.label(format!("{:.4}", pos.sol_spent)); });
                            row.col(|ui| { ui.label(format!("{}s", pos.age_seconds())); });
                        });
                    }
                });
        });
    }

    fn history(&mut self, ui: &mut egui::Ui) {
        let s = self.state.lock().unwrap();
        egui::ScrollArea::vertical().show(ui, |ui| {
            egui_extras::TableBuilder::new(ui)
                .striped(true)
                .column(egui_extras::Column::auto().at_least(70.0))
                .column(egui_extras::Column::auto().at_least(80.0))
                .column(egui_extras::Column::auto().at_least(50.0))
                .column(egui_extras::Column::auto().at_least(90.0))
                .column(egui_extras::Column::auto().at_least(90.0))
                .column(egui_extras::Column::auto().at_least(90.0))
                .column(egui_extras::Column::remainder())
                .header(20.0, |mut header| {
                    header.col(|ui| { ui.strong("Time"); });
                    header.col(|ui| { ui.strong("Symbol"); });
                    header.col(|ui| { ui.strong("Side"); });
                    header.col(|ui| { ui.strong("SOL amount"); });
                    header.col(|ui| { ui.strong("Fees (SOL)"); });
                    header.col(|ui| { ui.strong("Reason"); });
                    header.col(|ui| { ui.strong("Tx"); });
                })
                .body(|mut body| {
                    for t in s.history.iter().rev() {
                        body.row(20.0, |mut row| {
                            let time = chrono::DateTime::from_timestamp(t.timestamp, 0)
                                .map(|d| d.format("%H:%M:%S").to_string())
                                .unwrap_or_default();
                            row.col(|ui| { ui.label(time); });
                            row.col(|ui| { ui.label(&t.symbol); });
                            row.col(|ui| { ui.label(&t.side); });
                            row.col(|ui| { ui.label(format!("{:.4}", t.sol_amount)); });
                            row.col(|ui| { ui.label(format!("{:.6}", t.fees_sol)); });
                            row.col(|ui| { ui.label(&t.reason); });
                            row.col(|ui| {
                                match &t.tx_signature {
                                    Some(sig) => { ui.label(format!("{}...", &sig[..8.min(sig.len())])); }
                                    None => { ui.label("(dry run)"); }
                                }
                            });
                        });
                    }
                });
        });
    }

    fn active_coins(&mut self, ui: &mut egui::Ui) {
        let s = self.state.lock().unwrap();
        egui::ScrollArea::vertical().show(ui, |ui| {
            egui_extras::TableBuilder::new(ui)
                .striped(true)
                .column(egui_extras::Column::auto().at_least(70.0))
                .column(egui_extras::Column::auto().at_least(80.0))
                .column(egui_extras::Column::auto().at_least(80.0))
                .column(egui_extras::Column::remainder())
                .header(20.0, |mut header| {
                    header.col(|ui| { ui.strong("Time"); });
                    header.col(|ui| { ui.strong("Symbol"); });
                    header.col(|ui| { ui.strong("Dev held"); });
                    header.col(|ui| { ui.strong("Outcome"); });
                })
                .body(|mut body| {
                    for w in s.watched.iter() {
                        body.row(20.0, |mut row| {
                            let time = chrono::DateTime::from_timestamp(w.detected_at, 0)
                                .map(|d| d.format("%H:%M:%S").to_string())
                                .unwrap_or_default();
                            row.col(|ui| { ui.label(time); });
                            row.col(|ui| { ui.label(&w.symbol); });
                            row.col(|ui| {
                                match w.dev_holder_pct {
                                    Some(p) => { ui.label(format!("{p:.1}%")); }
                                    None => { ui.label("-"); }
                                }
                            });
                            row.col(|ui| {
                                if w.bought {
                                    ui.colored_label(egui::Color32::from_rgb(80, 200, 120), "bought");
                                } else if let Some(r) = &w.skipped_reason {
                                    ui.colored_label(egui::Color32::GRAY, format!("skipped: {r}"));
                                } else {
                                    ui.label("-");
                                }
                            });
                        });
                    }
                });
        });
    }

    fn settings(&mut self, ui: &mut egui::Ui) {
        ui.heading("Settings");
        ui.add_space(8.0);

        egui::Grid::new("settings_grid").num_columns(2).spacing([12.0, 8.0]).show(ui, |ui| {
            ui.label("RPC HTTP URL");
            ui.text_edit_singleline(&mut self.draft.rpc_http_url);
            ui.end_row();

            ui.label("RPC WebSocket URL");
            ui.text_edit_singleline(&mut self.draft.rpc_ws_url);
            ui.end_row();

            ui.label("Dry run (no real trades)");
            ui.checkbox(&mut self.draft.dry_run, "");
            ui.end_row();

            ui.label("Buy amount (SOL per trade)");
            ui.text_edit_singleline(&mut self.draft.buy_amount_sol);
            ui.end_row();

            ui.label("Max total SOL cap");
            ui.text_edit_singleline(&mut self.draft.max_total_sol_cap);
            ui.end_row();

            ui.label("Slippage (%)");
            ui.text_edit_singleline(&mut self.draft.slippage_pct);
            ui.end_row();

            ui.label("Take profit (%)");
            ui.text_edit_singleline(&mut self.draft.take_profit_pct);
            ui.end_row();

            ui.label("Stop loss (%)");
            ui.text_edit_singleline(&mut self.draft.stop_loss_pct);
            ui.end_row();

            ui.label("Max dev holder (%)");
            ui.text_edit_singleline(&mut self.draft.max_dev_holder_pct);
            ui.end_row();

            ui.label("Max hold time enabled");
            ui.checkbox(&mut self.draft.max_hold_enabled, "");
            ui.end_row();

            ui.label("Max hold seconds");
            ui.text_edit_singleline(&mut self.draft.max_hold_seconds);
            ui.end_row();
        });

        ui.add_space(12.0);
        if ui.button("Save").clicked() {
            self.save_settings();
        }
        if let Some(msg) = &self.settings_message {
            ui.colored_label(egui::Color32::from_rgb(80, 200, 120), msg);
        }
    }

    fn save_settings(&mut self) {
        let parse = |s: &str| s.trim().parse::<f64>().ok();

        let Some(buy_amount_sol) = parse(&self.draft.buy_amount_sol) else {
            self.settings_message = Some("Invalid buy amount".into());
            return;
        };
        let Some(max_total_sol_cap) = parse(&self.draft.max_total_sol_cap) else {
            self.settings_message = Some("Invalid max cap".into());
            return;
        };
        let Some(slippage_pct) = parse(&self.draft.slippage_pct) else {
            self.settings_message = Some("Invalid slippage".into());
            return;
        };
        let Some(take_profit_pct) = parse(&self.draft.take_profit_pct) else {
            self.settings_message = Some("Invalid take profit".into());
            return;
        };
        let Some(stop_loss_pct) = parse(&self.draft.stop_loss_pct) else {
            self.settings_message = Some("Invalid stop loss".into());
            return;
        };
        let Some(max_dev_holder_pct) = parse(&self.draft.max_dev_holder_pct) else {
            self.settings_message = Some("Invalid dev holder %".into());
            return;
        };
        let Some(max_hold_seconds) = parse(&self.draft.max_hold_seconds) else {
            self.settings_message = Some("Invalid max hold seconds".into());
            return;
        };

        let mut cfg = self.cfg.lock().unwrap();
        cfg.rpc_http_url = self.draft.rpc_http_url.clone();
        cfg.rpc_ws_url = self.draft.rpc_ws_url.clone();
        cfg.dry_run = self.draft.dry_run;
        cfg.buy_amount_sol = buy_amount_sol;
        cfg.max_total_sol_cap = max_total_sol_cap;
        cfg.slippage_bps = (slippage_pct * 100.0).round() as u64;
        cfg.take_profit_pct = take_profit_pct.clamp(0.0, 100.0);
        cfg.stop_loss_pct = stop_loss_pct.clamp(0.0, 100.0);
        cfg.max_dev_holder_pct = max_dev_holder_pct;
        cfg.max_hold_enabled = self.draft.max_hold_enabled;
        cfg.max_hold_seconds = max_hold_seconds as u64;

        let save_result = cfg.save(&self.config_path);
        let dry_run = cfg.dry_run;
        drop(cfg);

        let mut s = self.state.lock().unwrap();
        s.dry_run = dry_run;
        match save_result {
            Ok(_) => {
                s.push_log("Settings saved.".to_string());
                self.settings_message = Some("Saved.".into());
            }
            Err(e) => {
                self.settings_message = Some(format!("Save failed: {e}"));
            }
        }
    }

    fn log(&mut self, ui: &mut egui::Ui) {
        let s = self.state.lock().unwrap();
        egui::ScrollArea::vertical().stick_to_bottom(true).show(ui, |ui| {
            for line in s.log.iter() {
                ui.monospace(line);
            }
        });
    }
}
