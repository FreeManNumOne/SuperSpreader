use crate::{config::Settings, feed_handler::Tob};

#[derive(Debug, Clone)]
pub struct RiskDecision {
    pub ok: bool,
    pub reason: Option<&'static str>,
}

pub struct RiskEngine {
    settings: Settings,
}

impl RiskEngine {
    pub fn new(settings: Settings) -> Self {
        Self { settings }
    }

    pub fn can_quote(
        &self,
        tob: &Tob,
        now_ts: f64,
        is_active_market: bool,
        min_profitable_spread_bps: f64,
    ) -> RiskDecision {
        let feed_lag_ms = ((now_ts - tob.ts).max(0.0) * 1000.0) as u64;
        if feed_lag_ms > self.settings.reject_feed_lag_ms {
            return RiskDecision {
                ok: false,
                reason: Some("feed_lag"),
            };
        }

        let stale_secs = (now_ts - tob.ts).max(0.0);
        if !is_active_market && stale_secs > self.settings.max_feed_lag_secs {
            return RiskDecision {
                ok: false,
                reason: Some("feed_lag_max"),
            };
        }

        let (Some(bid), Some(ask)) = (tob.best_bid, tob.best_ask) else {
            return RiskDecision {
                ok: false,
                reason: Some("no_tob"),
            };
        };
        if ask <= bid {
            return RiskDecision {
                ok: false,
                reason: Some("crossed"),
            };
        }

        let mid = 0.5 * (ask + bid);
        if mid <= 0.0 {
            return RiskDecision {
                ok: false,
                reason: Some("bad_mid"),
            };
        }
        let spread_bps = ((ask - bid) / mid) * 10_000.0;
        if spread_bps < min_profitable_spread_bps {
            return RiskDecision {
                ok: false,
                reason: Some("unprofitable_spread"),
            };
        }

        let total = tob.bid_depth_5 + tob.ask_depth_5;
        let imbalance = if total > 0.0 {
            (tob.bid_depth_5 - tob.ask_depth_5) / total
        } else {
            0.0
        };
        if imbalance.abs() > self.settings.reject_abs_imbalance {
            return RiskDecision {
                ok: false,
                reason: Some("imbalance"),
            };
        }

        RiskDecision {
            ok: true,
            reason: None,
        }
    }
}
