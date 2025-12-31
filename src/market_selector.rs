 use anyhow::{Context, Result};
 use serde::{Deserialize, Serialize};
 
 use crate::{config::Settings, feed_handler::FeedState, store::SqliteStore, utils::now_ts};
 
 use polymarket_hft::client::polymarket::gamma::GetMarketsRequest;
 use polymarket_hft::client::polymarket::gamma::Client as GammaClient;
 
 #[derive(Debug, Clone, Serialize, Deserialize)]
 pub struct SelectedMarket {
     pub market_id: String,
     pub question: Option<String>,
     pub event_id: Option<String>,
     pub end_ts: Option<f64>,
     pub volume_24h_usd: f64,
     pub liquidity_usd: f64,
     pub condition_id: Option<String>,
     pub clob_token_id: Option<String>,
     pub microstructure_score: f64,
 }
 
 pub struct MarketSelector {
     settings: Settings,
     store: SqliteStore,
     feed: FeedState,
 }
 
 impl MarketSelector {
     pub fn new(settings: Settings, store: SqliteStore, feed: FeedState) -> Self {
         Self { settings, store, feed }
     }
 
     /// Discover and rank markets using live Gamma.
     pub async fn select(&self) -> Result<Vec<SelectedMarket>> {
         let ts = now_ts();
 
         let gamma = GammaClient::with_base_url(&self.settings.gamma_base_url)
             .with_context(|| format!("gamma base url {}", self.settings.gamma_base_url))?;
 
         let req = GetMarketsRequest {
             limit: Some(1000),
             offset: Some(0),
             closed: Some(false),
             ..Default::default()
         };
 
         let markets = gamma
             .get_markets(req)
             .await
             .context("gamma.get_markets")?;
 
         let mut eligible: Vec<SelectedMarket> = Vec::new();
         let mut eligible_ids: Vec<String> = Vec::new();
 
         for m in markets {
             let active = m.active.unwrap_or(true) && !m.closed.unwrap_or(false);
             if !active {
                 continue;
             }
 
             let market_id = m.id.clone();
             let volume_24h_usd = m
                 .volume24hr_clob
                 .or(m.volume24hr)
                 .or(m.volume_num)
                 .unwrap_or(0.0);
             let liquidity_usd = m.liquidity_num.or(m.liquidity_clob).unwrap_or(0.0);
 
             if volume_24h_usd < self.settings.min_24h_volume_usd || liquidity_usd < self.settings.min_liquidity_usd {
                 continue;
             }
 
             let condition_id = m.condition_id.clone();
             let clob_token_id = pick_primary_token_id(m.clob_token_ids.as_deref(), m.outcomes.as_deref());
             if clob_token_id.is_none() {
                 // Can't subscribe/trade without a token id.
                 continue;
             }
 
             // end_ts: parse RFC3339 if provided
             let end_ts = m
                 .end_date_iso
                 .as_deref()
                 .or(m.end_date.as_deref())
                 .and_then(parse_ts_rfc3339);
 
             let event_id = m
                 .events
                 .as_ref()
                 .and_then(|evs| evs.first())
                 .map(|e| e.id.clone())
                 .unwrap_or_else(|| format!("event:{market_id}"));
 
             // Microstructure metrics from current feed snapshot (may be missing early on).
             let (spread_bps, imbalance_abs, updates_per_min) = self
                 .feed
                 .get(&market_id)
                 .and_then(|tob| {
                     let (Some(b), Some(a)) = (tob.best_bid, tob.best_ask) else { return None; };
                     if a <= b {
                         return None;
                     }
                     let mid = 0.5 * (a + b);
                     if !(mid > 0.0) {
                         return None;
                     }
                     let spread_bps = ((a - b) / mid) * 10_000.0;
                     let total = tob.bid_depth_5 + tob.ask_depth_5;
                     let imb = if total > 0.0 { (tob.bid_depth_5 - tob.ask_depth_5) / total } else { 0.0 };
                     Some((spread_bps, imb.abs(), tob.updates_ewma_per_min))
                 })
                 .unwrap_or((0.0, 0.0, 0.0));
 
             // Score per spec: (book_updates/min * spread_bps) + imbalance_ratio
             let microstructure_score = (updates_per_min * spread_bps) + imbalance_abs;
 
             // For selection, require minimum spread and update rate *if we have them*.
             // If feed hasn't warmed up yet, allow these markets to seed subscriptions.
             let has_metrics = updates_per_min > 0.0 && spread_bps > 0.0;
             if has_metrics {
                 if spread_bps < self.settings.min_spread_bps || updates_per_min < self.settings.min_updates_min {
                     continue;
                 }
             }
 
             // Persist to SQLite markets table (dashboard depends on this).
             self.store
                 .upsert_market(
                     &market_id,
                     m.question.as_deref(),
                     Some(&event_id),
                     true,
                     end_ts,
                     volume_24h_usd,
                     liquidity_usd,
                     condition_id.as_deref(),
                     clob_token_id.as_deref(),
                     ts,
                 )
                 .ok();
 
             eligible_ids.push(market_id.clone());
             eligible.push(SelectedMarket {
                 market_id,
                 question: m.question,
                 event_id: Some(event_id),
                 end_ts,
                 volume_24h_usd,
                 liquidity_usd,
                 condition_id,
                 clob_token_id,
                 microstructure_score,
             });
         }
 
         // Sort by microstructure score (fallback tie-breakers: volume/liquidity).
         eligible.sort_by(|a, b| {
             b.microstructure_score
                 .partial_cmp(&a.microstructure_score)
                 .unwrap_or(std::cmp::Ordering::Equal)
                 .then_with(|| b.volume_24h_usd.partial_cmp(&a.volume_24h_usd).unwrap_or(std::cmp::Ordering::Equal))
                 .then_with(|| b.liquidity_usd.partial_cmp(&a.liquidity_usd).unwrap_or(std::cmp::Ordering::Equal))
         });
 
         let top_n = self.settings.top_n_markets.min(self.settings.max_markets_subscribed);
         let selected = eligible.into_iter().take(top_n).collect::<Vec<_>>();
 
         // Persist scanner/watchlist.
         self.store
             .insert_scanner_snapshot(ts, eligible_ids.len() as i64, selected.len() as i64)
             .ok();
         self.store
             .update_watchlist(&selected.iter().map(|m| m.market_id.clone()).collect::<Vec<_>>(), ts)
             .ok();
 
         Ok(selected)
     }
 }
 
 fn parse_ts_rfc3339(s: &str) -> Option<f64> {
     chrono::DateTime::parse_from_rfc3339(s)
         .ok()
         .map(|dt| dt.timestamp_millis() as f64 / 1000.0)
 }
 
 fn parse_listish(s: &str) -> Vec<String> {
     // Accept JSON-ish arrays represented as strings, ex: ["Yes","No"] or ['1','2'] or 1,2.
     let t = s.trim();
     let t = t.trim_start_matches('[').trim_end_matches(']');
     t.split(',')
         .map(|x| x.trim().trim_matches('"').trim_matches('\'').to_string())
         .filter(|x| !x.is_empty())
         .collect()
 }
 
 fn pick_primary_token_id(clob_token_ids: Option<&str>, outcomes: Option<&str>) -> Option<String> {
     let toks = clob_token_ids.map(parse_listish).unwrap_or_default();
     if toks.is_empty() {
         return None;
     }
     let outs = outcomes.map(parse_listish).unwrap_or_default();
     if !outs.is_empty() && outs.len() == toks.len() {
         for (i, o) in outs.iter().enumerate() {
             if o.trim().eq_ignore_ascii_case("yes") {
                 return Some(toks[i].clone());
             }
         }
     }
     Some(toks[0].clone())
 }
