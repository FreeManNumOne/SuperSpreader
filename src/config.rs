use std::env;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

fn get_env(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn get_env_bool(key: &str, default: bool) -> bool {
    match get_env(key) {
        None => default,
        Some(v) => matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "y" | "on"),
    }
}

fn get_env_f64(key: &str, default: f64) -> Result<f64> {
    match get_env(key) {
        None => Ok(default),
        Some(v) => Ok(v
            .parse::<f64>()
            .map_err(|e| anyhow!("{key} invalid float: {e}"))?),
    }
}

fn get_env_usize(key: &str, default: usize) -> Result<usize> {
    match get_env(key) {
        None => Ok(default),
        Some(v) => Ok(v
            .parse::<usize>()
            .map_err(|e| anyhow!("{key} invalid int: {e}"))?),
    }
}

fn get_env_string(key: &str, default: &str) -> String {
    get_env(key).unwrap_or_else(|| default.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    // Modes
    pub trade_mode: String,     // paper only supported here
    pub execution_mode: String, // paper|shadow
    pub run_mode: String,       // paper|scanner

    pub disallow_mock_data: bool,

    // Network
    pub clob_ws_url: String,
    pub gamma_base_url: String,

    // Market selection
    pub top_n_markets: usize,
    pub min_24h_volume_usd: f64,
    pub min_liquidity_usd: f64,
    pub min_spread_bps: f64,
    pub min_updates_min: f64,
    pub market_refresh_secs: u64,
    pub max_markets_subscribed: usize,

    // Costs / profitability guardrail
    pub fees_bps: f64,
    pub slippage_bps: f64,
    pub latency_bps: f64,

    // Risk
    pub max_feed_lag_secs: f64,
    pub reject_feed_lag_ms: u64,
    pub reject_abs_imbalance: f64,
    pub max_inventory_usd: f64,

    // Strategy knobs
    pub price_tick: f64,
    pub mm_quote_width: f64,
    pub mm_levels: usize,
    pub mm_min_quote_life_secs: f64,
    pub mm_reprice_threshold: f64,
    pub inventory_skew_cap: f64,
    pub base_order_size: f64,

    // Paper realism
    pub paper_fill_model: String,
    pub paper_min_rest_secs: f64,
    pub paper_poisson_lambda_per_sec: f64,
    pub paper_fault_rate: f64,
    pub paper_non_atomic_fail_rate: f64,
    pub paper_rehydrate_portfolio: bool,
    pub paper_reset_on_start: bool,

    // Telemetry / storage / dashboard
    pub sqlite_path: String,
    pub dashboard_enabled: bool,
    pub dashboard_host: String,
    pub dashboard_port: u16,
    pub dashboard_enable_reset: bool,
    pub dashboard_open_browser: bool,

    // Loop timing
    pub loop_ms: u64,
    pub eval_interval_secs: u64,
}

impl Settings {
    pub fn load() -> Result<Self> {
        let trade_mode = get_env_string("TRADE_MODE", "paper").to_lowercase();
        let execution_mode = get_env_string("EXECUTION_MODE", "paper").to_lowercase();
        let run_mode = get_env_string("RUN_MODE", "paper").to_lowercase();

        if trade_mode != "paper" {
            return Err(anyhow!(
                "This Rust bot is paper-only. Set TRADE_MODE=paper (got {trade_mode})"
            ));
        }
        if !matches!(execution_mode.as_str(), "paper" | "shadow") {
            return Err(anyhow!("EXECUTION_MODE must be paper|shadow"));
        }
        if !matches!(run_mode.as_str(), "paper" | "scanner") {
            return Err(anyhow!("RUN_MODE must be paper|scanner"));
        }

        let disallow_mock_data = get_env_bool("DISALLOW_MOCK_DATA", true);

        let clob_ws_url = get_env_string(
            "POLYMARKET_WS",
            "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        );
        let gamma_base_url = get_env_string("GAMMA_BASE_URL", "https://gamma-api.polymarket.com");

        let fees_bps = get_env_f64("FEES_BPS", 0.0)?;
        let slippage_bps = get_env_f64("SLIPPAGE_BPS", 20.0)?;
        let latency_bps = get_env_f64("LATENCY_BPS", 10.0)?;

        let paper_fill_model = get_env_string("PAPER_FILL_MODEL", "maker_touch").to_lowercase();
        if paper_fill_model != "maker_touch" {
            return Err(anyhow!(
                "Only PAPER_FILL_MODEL=maker_touch is supported (got {paper_fill_model})"
            ));
        }

        let paper_min_rest_secs = get_env_f64("PAPER_MIN_REST_SECS", 1.0)?;

        let s = Self {
            trade_mode,
            execution_mode,
            run_mode,
            disallow_mock_data,
            clob_ws_url,
            gamma_base_url,
            top_n_markets: get_env_usize("TOP_N_MARKETS", 50)?,
            min_24h_volume_usd: get_env_f64("MIN_24H_VOLUME_USD", 10_000.0)?,
            min_liquidity_usd: get_env_f64("MIN_LIQUIDITY_USD", 20_000.0)?,
            min_spread_bps: get_env_f64("MIN_SPREAD_BPS", 10.0)?,
            min_updates_min: get_env_f64("MIN_UPDATES_MIN", 5.0)?,
            market_refresh_secs: get_env_f64("MARKET_REFRESH_SECS", 60.0)? as u64,
            max_markets_subscribed: get_env_usize("MAX_MARKETS_SUBSCRIBED", 30)?,
            fees_bps,
            slippage_bps,
            latency_bps,
            max_feed_lag_secs: get_env_f64("MAX_FEED_LAG_SECS", 300.0)?,
            reject_feed_lag_ms: get_env_usize("REJECT_FEED_LAG_MS", 100)? as u64,
            reject_abs_imbalance: get_env_f64("REJECT_ABS_IMBALANCE", 0.5)?,
            max_inventory_usd: get_env_f64("MAX_INVENTORY_USD", 5000.0)?,
            price_tick: get_env_f64("PRICE_TICK", 0.001)?,
            mm_quote_width: get_env_f64("MM_QUOTE_WIDTH", 0.02)?,
            mm_levels: get_env_usize("MM_LEVELS", 7)?,
            mm_min_quote_life_secs: get_env_f64("MM_MIN_QUOTE_LIFE_SECS", 5.0)?,
            mm_reprice_threshold: get_env_f64("MM_REPRICE_THRESHOLD", 0.005)?,
            inventory_skew_cap: get_env_f64("INVENTORY_SKEW_CAP", 0.003)?,
            base_order_size: get_env_f64("BASE_ORDER_SIZE", 10.0)?,
            paper_fill_model,
            paper_min_rest_secs,
            paper_poisson_lambda_per_sec: get_env_f64("PAPER_POISSON_LAMBDA_PER_SEC", 0.5)?,
            paper_fault_rate: get_env_f64("PAPER_FAULT_RATE", 0.08)?,
            paper_non_atomic_fail_rate: get_env_f64("PAPER_NON_ATOMIC_FAIL_RATE", 0.02)?,
            paper_rehydrate_portfolio: get_env_bool("PAPER_REHYDRATE_PORTFOLIO", true),
            paper_reset_on_start: get_env_bool("PAPER_RESET_ON_START", false),
            sqlite_path: get_env_string("SQLITE_PATH", "./data/polymarket_trader.sqlite"),
            dashboard_enabled: get_env_bool("DASHBOARD_ENABLED", true),
            dashboard_host: get_env_string("DASHBOARD_HOST", "127.0.0.1"),
            dashboard_port: get_env_usize("DASHBOARD_PORT", 8000)? as u16,
            dashboard_enable_reset: get_env_bool("DASHBOARD_ENABLE_RESET", false),
            dashboard_open_browser: get_env_bool("DASHBOARD_OPEN_BROWSER", true),
            loop_ms: get_env_usize("LOOP_MS", 50)? as u64,
            eval_interval_secs: get_env_usize("EVAL_INTERVAL_SECS", 600)? as u64,
        };

        s.validate()?;
        Ok(s)
    }

    pub fn cost_bps(&self) -> f64 {
        self.fees_bps + self.slippage_bps + self.latency_bps
    }

    pub fn validate(&self) -> Result<()> {
        if !self.price_tick.is_finite() || self.price_tick <= 0.0 {
            return Err(anyhow!("PRICE_TICK must be > 0 (got {})", self.price_tick));
        }
        if !self.mm_quote_width.is_finite() || self.mm_quote_width < self.price_tick {
            return Err(anyhow!(
                "MM_QUOTE_WIDTH must be >= PRICE_TICK (mm_quote_width={} price_tick={})",
                self.mm_quote_width,
                self.price_tick
            ));
        }
        if self.mm_levels < 1 {
            return Err(anyhow!("MM_LEVELS must be >= 1 (got {})", self.mm_levels));
        }
        if self.loop_ms < 1 {
            return Err(anyhow!("LOOP_MS must be >= 1 (got {})", self.loop_ms));
        }
        if self.market_refresh_secs < 1 {
            return Err(anyhow!(
                "MARKET_REFRESH_SECS must be >= 1 (got {})",
                self.market_refresh_secs
            ));
        }
        if self.eval_interval_secs < 1 {
            return Err(anyhow!(
                "EVAL_INTERVAL_SECS must be >= 1 (got {})",
                self.eval_interval_secs
            ));
        }
        if self.max_markets_subscribed < 1 {
            return Err(anyhow!(
                "MAX_MARKETS_SUBSCRIBED must be >= 1 (got {})",
                self.max_markets_subscribed
            ));
        }
        if !self.max_feed_lag_secs.is_finite() || self.max_feed_lag_secs <= 0.0 {
            return Err(anyhow!(
                "MAX_FEED_LAG_SECS must be > 0 (got {})",
                self.max_feed_lag_secs
            ));
        }
        if !self.mm_min_quote_life_secs.is_finite() || self.mm_min_quote_life_secs < 0.0 {
            return Err(anyhow!(
                "MM_MIN_QUOTE_LIFE_SECS must be >= 0 (got {})",
                self.mm_min_quote_life_secs
            ));
        }
        if !self.base_order_size.is_finite() || self.base_order_size <= 0.0 {
            return Err(anyhow!(
                "BASE_ORDER_SIZE must be > 0 (got {})",
                self.base_order_size
            ));
        }
        Ok(())
    }
}
