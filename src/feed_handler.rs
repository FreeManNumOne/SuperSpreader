use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use parking_lot::RwLock;

use crate::{
    config::Settings,
    market_selector::SelectedMarket,
    utils::{ewma, now_ts},
};

use polymarket_hft::client::polymarket::clob::orderbook::GetOrderBooksRequestItem;
use polymarket_hft::client::polymarket::clob::ws::ClobWsClient;
use polymarket_hft::client::polymarket::clob::ws::WsMessage;
use polymarket_hft::client::polymarket::clob::Client as ClobClient;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct Tob {
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub bid_depth_5: f64,
    pub ask_depth_5: f64,
    pub ts: f64,
    pub updates_ewma_per_min: f64,
    pub last_trade_ema: Option<f64>,
    pub last_trade_ts: Option<f64>,
}

impl Tob {
    pub fn mid(&self) -> Option<f64> {
        match (self.best_bid, self.best_ask) {
            (Some(b), Some(a)) if a > 0.0 && b > 0.0 => Some(0.5 * (a + b)),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct FeedState {
    inner: std::sync::Arc<RwLock<HashMap<String, Tob>>>,
}

impl FeedState {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[allow(dead_code)]
    pub fn upsert(&self, market_id: &str, tob: Tob) {
        self.inner.write().insert(market_id.to_string(), tob);
    }

    /// Update book fields in-place under a single write lock.
    ///
    /// - Preserves last trade fields.
    /// - Optionally updates updates/min EWMA when `inst_updates_per_min` is provided.
    #[allow(clippy::too_many_arguments)]
    pub fn update_book_owned(
        &self,
        market_id: &str,
        ts: f64,
        best_bid: Option<f64>,
        best_ask: Option<f64>,
        bid_depth_5: f64,
        ask_depth_5: f64,
        inst_updates_per_min: Option<f64>,
    ) {
        let mut m = self.inner.write();
        let e = m.entry(market_id.to_string()).or_insert(Tob {
            best_bid: None,
            best_ask: None,
            bid_depth_5: 0.0,
            ask_depth_5: 0.0,
            ts,
            updates_ewma_per_min: 0.0,
            last_trade_ema: None,
            last_trade_ts: None,
        });

        e.best_bid = best_bid;
        e.best_ask = best_ask;
        e.bid_depth_5 = bid_depth_5;
        e.ask_depth_5 = ask_depth_5;
        e.ts = ts;

        if let Some(inst) = inst_updates_per_min {
            e.updates_ewma_per_min = ewma(Some(e.updates_ewma_per_min), inst, 0.1);
        }
    }

    /// Update last trade fields in-place under a single write lock.
    ///
    /// Does NOT overwrite `tob.ts` (book freshness).
    pub fn update_last_trade_owned(&self, market_id: &str, px: f64, trade_ts: f64) {
        let mut m = self.inner.write();
        let e = m.entry(market_id.to_string()).or_insert(Tob {
            best_bid: None,
            best_ask: None,
            bid_depth_5: 0.0,
            ask_depth_5: 0.0,
            ts: 0.0,
            updates_ewma_per_min: 0.0,
            last_trade_ema: None,
            last_trade_ts: None,
        });

        e.last_trade_ema = Some(ewma(e.last_trade_ema, px, 0.2));
        e.last_trade_ts = Some(trade_ts);
    }

    pub fn get(&self, market_id: &str) -> Option<Tob> {
        self.inner.read().get(market_id).cloned()
    }

    #[allow(dead_code)]
    pub fn snapshot(&self) -> HashMap<String, Tob> {
        self.inner.read().clone()
    }
}

#[derive(Default, Clone)]
struct Routes {
    // condition_id (0x...) -> market_id (gamma numeric string)
    by_condition: HashMap<String, String>,
    // asset_id (token id) -> market_id
    by_asset: HashMap<String, String>,
    // market_id -> primary token id
    token_for_market: HashMap<String, String>,
}

pub struct FeedHandler {
    settings: Settings,
    pub state: FeedState,
    routes: std::sync::Arc<RwLock<Routes>>,
}

impl FeedHandler {
    pub fn new(settings: Settings) -> Self {
        Self {
            settings,
            state: FeedState::new(),
            routes: std::sync::Arc::new(RwLock::new(Routes::default())),
        }
    }

    pub fn state(&self) -> FeedState {
        self.state.clone()
    }

    pub fn spawn(
        self,
        selected_rx: watch::Receiver<Arc<Vec<SelectedMarket>>>,
        store: crate::store::SqliteStore,
    ) {
        let routes = self.routes.clone();
        let state = self.state.clone();
        let settings = self.settings.clone();

        // Watchlist router updater
        let store_routes = store.clone();
        let mut selected_routes_rx = selected_rx.clone();
        tokio::spawn(async move {
            loop {
                if selected_routes_rx.changed().await.is_err() {
                    break;
                }
                let markets = selected_routes_rx.borrow().clone(); // Arc clone (cheap)
                let mut r = routes.write();
                r.by_condition.clear();
                r.by_asset.clear();
                r.token_for_market.clear();

                for m in markets.iter() {
                    if let Some(cid) = m
                        .condition_id
                        .as_ref()
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                    {
                        r.by_condition.insert(cid.to_string(), m.market_id.clone());
                    }
                    if let Some(tid) = m
                        .clob_token_id
                        .as_ref()
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                    {
                        r.by_asset.insert(tid.to_string(), m.market_id.clone());
                        r.token_for_market
                            .insert(m.market_id.clone(), tid.to_string());
                    }
                }

                store_routes
                    .upsert_runtime_status(
                        "feed.routes",
                        "ok",
                        &format!("routes updated (markets={})", markets.len()),
                        None,
                        now_ts(),
                    )
                    .ok();
            }
        });

        // WS reader loop
        let routes_ws = self.routes.clone();
        let state_ws = state.clone();
        let settings_ws = settings.clone();
        let store_ws = store.clone();
        let selected_ws_rx = selected_rx.clone();
        tokio::spawn(async move {
            if let Err(e) =
                run_ws_loop(settings_ws, state_ws, routes_ws, selected_ws_rx, store_ws).await
            {
                log::error!("feed.ws.loop.error {}", e);
            }
        });

        // CLOB orderbook polling loop (freshness + safety net)
        let routes_poll = self.routes.clone();
        let state_poll = state.clone();
        let settings_poll = settings.clone();
        let store_poll = store.clone();
        tokio::spawn(async move {
            if let Err(e) = run_poll_loop(
                settings_poll,
                state_poll,
                routes_poll,
                selected_rx,
                store_poll,
            )
            .await
            {
                log::error!("feed.poll.loop.error {}", e);
            }
        });
    }
}

fn ws_base_url(ws_url: &str) -> String {
    // Accept either:
    // - wss://ws-subscriptions-clob.polymarket.com
    // - wss://ws-subscriptions-clob.polymarket.com/ws/market
    let s = ws_url.trim();
    if let Some((base, _)) = s.split_once("/ws/") {
        base.to_string()
    } else {
        s.trim_end_matches('/').to_string()
    }
}

async fn run_ws_loop(
    settings: Settings,
    state: FeedState,
    routes: std::sync::Arc<RwLock<Routes>>,
    mut selected_rx: watch::Receiver<Arc<Vec<SelectedMarket>>>,
    store: crate::store::SqliteStore,
) -> Result<()> {
    let mut ws = ClobWsClient::builder()
        .base_url(ws_base_url(&settings.clob_ws_url))
        .build();
    let mut last_subscribed: Vec<String> = vec![];
    let mut needs_resubscribe = true;
    let mut force_resubscribe = true;

    // Per-market last update for EWMA.
    let mut last_update_ts: HashMap<String, f64> = HashMap::new();

    loop {
        // Event-driven subscription updates: only (re)calculate and subscribe when selection changes
        // or after a reconnect/drop.
        if needs_resubscribe {
            // Clone the Arc out of the watch ref so we don't hold a non-Send borrow across .await.
            let selected = selected_rx.borrow().clone();
            let mut desired_sorted: Vec<String> = selected
                .iter()
                .filter_map(|m| m.clob_token_id.as_deref())
                .map(|s| s.to_string())
                .collect();
            desired_sorted.sort();
            desired_sorted.dedup();

            let selection_changed = desired_sorted != last_subscribed;
            let should_resubscribe = selection_changed || force_resubscribe;
            if selection_changed {
                last_subscribed = desired_sorted;
            }

            if !should_resubscribe {
                needs_resubscribe = false;
                force_resubscribe = false;
                continue;
            }

            if last_subscribed.is_empty() {
                ws.disconnect().await;
                store
                    .upsert_runtime_status("feed.ws", "warn", "no markets selected", None, now_ts())
                    .ok();
            } else {
                store
                    .upsert_runtime_status(
                        "feed.ws",
                        "ok",
                        &format!("subscribing tokens={}", last_subscribed.len()),
                        Some(&format!("base_url={}", ws_base_url(&settings.clob_ws_url))),
                        now_ts(),
                    )
                    .ok();
                ws.subscribe_market(last_subscribed.clone())
                    .await
                    .map_err(|e| anyhow::anyhow!("ws.subscribe_market failed: {e}"))?;
            }

            needs_resubscribe = false;
            force_resubscribe = false;
        }

        // If we aren't subscribed to anything, just wait for the selection to change.
        if last_subscribed.is_empty() {
            if selected_rx.changed().await.is_err() {
                break;
            }
            needs_resubscribe = true;
            continue;
        }

        tokio::select! {
            _ = selected_rx.changed() => {
               needs_resubscribe = true;
               force_resubscribe = false;
            }
           msg = ws.next_message() => {
                let Some(msg) = msg else {
                    // Connection dropped and auto-reconnect may have failed; wait for next tick.
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                   needs_resubscribe = true;
                   force_resubscribe = true;
                   continue;
                };
                handle_ws_message(&state, &routes, &mut last_update_ts, msg)?;
            }
        }
    }

    Ok(())
}

fn handle_ws_message(
    state: &FeedState,
    routes: &std::sync::Arc<RwLock<Routes>>,
    last_update_ts: &mut HashMap<String, f64>,
    msg: WsMessage,
) -> Result<()> {
    match msg {
        WsMessage::Book(b) => {
            let market_id = {
                let r = routes.read();
                r.by_condition
                    .get(b.market.trim())
                    .cloned()
                    .or_else(|| r.by_asset.get(b.asset_id.trim()).cloned())
            };
            let Some(mid) = market_id else {
                return Ok(());
            };

            let ts = parse_ws_ts(&b.timestamp).unwrap_or_else(now_ts);
            let (best_bid, bid_depth_5) = parse_side_levels(&b.bids, true);
            let (best_ask, ask_depth_5) = parse_side_levels(&b.asks, false);

            let inst_per_min = last_update_ts
                .get(&mid)
                .and_then(|prev_ts| {
                    let dt = (ts - *prev_ts).max(0.0);
                    if dt <= 0.0 {
                        None
                    } else {
                        Some(60.0 / dt)
                    }
                })
                .unwrap_or(0.0);
            last_update_ts.insert(mid.clone(), ts);
            state.update_book_owned(
                &mid,
                ts,
                best_bid,
                best_ask,
                bid_depth_5,
                ask_depth_5,
                Some(inst_per_min),
            );
        }
        WsMessage::LastTradePrice(t) => {
            let market_id = {
                let r = routes.read();
                r.by_condition
                    .get(t.market.trim())
                    .cloned()
                    .or_else(|| r.by_asset.get(t.asset_id.trim()).cloned())
            };
            let Some(mid) = market_id else {
                return Ok(());
            };
            let px = t.price.parse::<f64>().ok();
            let ts = parse_ws_ts(&t.timestamp).unwrap_or_else(now_ts);
            if let Some(p) = px {
                state.update_last_trade_owned(&mid, p, ts);
            }
        }
        _ => {}
    }
    Ok(())
}

async fn run_poll_loop(
    _settings: Settings,
    state: FeedState,
    routes: std::sync::Arc<RwLock<Routes>>,
    mut selected_rx: watch::Receiver<Arc<Vec<SelectedMarket>>>,
    store: crate::store::SqliteStore,
) -> Result<()> {
    let clob = ClobClient::new();
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(15));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = selected_rx.changed() => {}
        }

        let tokens = {
            let r = routes.read();
            r.token_for_market
                .values()
                .cloned()
                .collect::<Vec<String>>()
        };
        if tokens.is_empty() {
            continue;
        }

        // Batch request for speed.
        //
        // IMPORTANT: do NOT set a side filter here.
        // If we request only BUY (or only SELL), many markets will not have both sides populated,
        // which in turn prevents computing a mid and causes the trading loop to skip quoting.
        let req = build_orderbooks_request(&tokens);

        let books = match clob.get_order_books(&req).await {
            Ok(b) => b,
            Err(e) => {
                store
                    .upsert_runtime_status(
                        "feed.poll",
                        "error",
                        "orderbook_poll_failed",
                        Some(&e.to_string()),
                        now_ts(),
                    )
                    .ok();
                continue;
            }
        };

        store
            .upsert_runtime_status(
                "feed.poll",
                "ok",
                &format!("polled {}", books.len()),
                None,
                now_ts(),
            )
            .ok();

        let now = now_ts();
        for b in books {
            let mid = {
                let r = routes.read();
                r.by_condition
                    .get(b.market.trim())
                    .cloned()
                    .or_else(|| r.by_asset.get(b.asset_id.trim()).cloned())
            };
            let Some(market_id) = mid else {
                continue;
            };

            let (best_bid, bid_depth_5) = parse_side_levels_ob(&b.bids, true);
            let (best_ask, ask_depth_5) = parse_side_levels_ob(&b.asks, false);

            // Preserve updates EWMA and trade fields; refresh only book fields and timestamp.
            let ts = now.max(parse_ws_ts(&b.timestamp).unwrap_or(now));
            state.update_book_owned(
                &market_id,
                ts,
                best_bid,
                best_ask,
                bid_depth_5,
                ask_depth_5,
                None,
            );
        }
    }
}

fn build_orderbooks_request(tokens: &[String]) -> Vec<GetOrderBooksRequestItem> {
    tokens
        .iter()
        .map(|t| GetOrderBooksRequestItem {
            token_id: t.clone(),
            side: None,
        })
        .collect::<Vec<_>>()
}

fn parse_ws_ts(s: &str) -> Option<f64> {
    let raw = s.trim().parse::<f64>().ok()?;
    // WS sometimes emits milliseconds; be defensive.
    if raw > 3_000_000_000_000.0 {
        Some(raw / 1000.0)
    } else if raw > 3_000_000_000.0 {
        // Could be milliseconds since epoch in seconds range; leave it.
        Some(raw)
    } else {
        Some(raw)
    }
}

fn parse_side_levels(
    levels: &[polymarket_hft::client::polymarket::clob::ws::WsPriceLevel],
    is_bid: bool,
) -> (Option<f64>, f64) {
    top5_by_price(
        levels.iter().filter_map(|lvl| {
            let px = lvl.price.parse::<f64>().ok()?;
            let sz = lvl.size.parse::<f64>().ok()?;
            if !px.is_finite() || !sz.is_finite() || px <= 0.0 || sz <= 0.0 {
                return None;
            }
            Some((px, sz))
        }),
        is_bid,
    )
}

fn parse_side_levels_ob(
    levels: &[polymarket_hft::client::polymarket::clob::orderbook::PriceLevel],
    is_bid: bool,
) -> (Option<f64>, f64) {
    top5_by_price(
        levels.iter().filter_map(|lvl| {
            let px = lvl.price.parse::<f64>().ok()?;
            let sz = lvl.size.parse::<f64>().ok()?;
            if !px.is_finite() || !sz.is_finite() || px <= 0.0 || sz <= 0.0 {
                return None;
            }
            Some((px, sz))
        }),
        is_bid,
    )
}

fn top5_by_price<I>(levels: I, is_bid: bool) -> (Option<f64>, f64)
where
    I: IntoIterator<Item = (f64, f64)>,
{
    #[inline]
    fn better_price(is_bid: bool, a: f64, b: f64) -> bool {
        if is_bid {
            a > b
        } else {
            a < b
        }
    }

    let mut buf: [(f64, f64); 5] = [(0.0, 0.0); 5];
    let mut n: usize = 0;

    for (px, sz) in levels {
        let mut pos = 0usize;
        while pos < n {
            if better_price(is_bid, px, buf[pos].0) {
                break;
            }
            pos += 1;
        }

        if n < 5 {
            // Insert into [0..n], shifting right.
            for j in (pos..n).rev() {
                buf[j + 1] = buf[j];
            }
            buf[pos] = (px, sz);
            n += 1;
        } else {
            // Full: only insert if it's better than the current worst (at index 4).
            if pos >= 5 {
                continue;
            }
            for j in (pos..4).rev() {
                buf[j + 1] = buf[j];
            }
            buf[pos] = (px, sz);
        }
    }

    if n == 0 {
        return (None, 0.0);
    }
    let best = Some(buf[0].0);
    let depth = buf.iter().take(n).map(|x| x.1).sum::<f64>();
    (best, depth)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_hft::client::polymarket::clob::ws::{
        BookMessage, LastTradePriceMessage, WsPriceLevel,
    };

    #[test]
    fn orderbook_poll_request_has_no_side_filter() {
        let tokens = vec!["t1".to_string(), "t2".to_string(), "t3".to_string()];
        let req = build_orderbooks_request(&tokens);
        assert_eq!(req.len(), 3);
        for r in req {
            assert!(r.side.is_none(), "poll request must not set side filter");
        }
    }

    #[test]
    fn ws_book_and_last_trade_events_update_tob_state() {
        let state = FeedState::new();
        let routes = std::sync::Arc::new(RwLock::new(Routes::default()));

        // Route a condition id to a numeric market id (what the rest of the bot uses).
        routes
            .write()
            .by_condition
            .insert("0xcond".to_string(), "516926".to_string());

        // First book snapshot.
        let msg1 = WsMessage::Book(BookMessage {
            event_type: "book".to_string(),
            asset_id: "token_yes".to_string(),
            market: "0xcond".to_string(),
            bids: vec![
                WsPriceLevel {
                    price: "0.49".to_string(),
                    size: "10".to_string(),
                },
                WsPriceLevel {
                    price: "0.48".to_string(),
                    size: "5".to_string(),
                },
            ],
            asks: vec![
                WsPriceLevel {
                    price: "0.51".to_string(),
                    size: "9".to_string(),
                },
                WsPriceLevel {
                    price: "0.52".to_string(),
                    size: "4".to_string(),
                },
            ],
            timestamp: "1700000000000".to_string(), // ms
            hash: "h".to_string(),
        });

        let mut last_update_ts: HashMap<String, f64> = HashMap::new();
        handle_ws_message(&state, &routes, &mut last_update_ts, msg1).unwrap();

        let tob = state.get("516926").expect("tob should be upserted");
        assert_eq!(tob.best_bid, Some(0.49));
        assert_eq!(tob.best_ask, Some(0.51));
        assert!(tob.mid().is_some());
        assert!(
            tob.ts > 1_000_000_000.0,
            "timestamp should be converted to seconds"
        );

        // Second book snapshot slightly later: should bump EWMA updates/min above zero.
        let msg2 = WsMessage::Book(BookMessage {
            event_type: "book".to_string(),
            asset_id: "token_yes".to_string(),
            market: "0xcond".to_string(),
            bids: vec![WsPriceLevel {
                price: "0.490".to_string(),
                size: "12".to_string(),
            }],
            asks: vec![WsPriceLevel {
                price: "0.510".to_string(),
                size: "11".to_string(),
            }],
            timestamp: "1700000000500".to_string(), // +500ms
            hash: "h2".to_string(),
        });
        handle_ws_message(&state, &routes, &mut last_update_ts, msg2).unwrap();
        let tob2 = state.get("516926").expect("tob should still exist");
        assert!(
            tob2.updates_ewma_per_min > 0.0,
            "should compute a non-zero updates/min EWMA"
        );

        // Last trade event: should update last_trade_ema/ts but not overwrite book ts.
        let trade = WsMessage::LastTradePrice(LastTradePriceMessage {
            event_type: "last_trade_price".to_string(),
            asset_id: "token_yes".to_string(),
            market: "0xcond".to_string(),
            price: "0.505".to_string(),
            side: polymarket_hft::client::polymarket::clob::ws::Side::Buy,
            size: "1.0".to_string(),
            fee_rate_bps: "0".to_string(),
            timestamp: "1700000000600".to_string(),
        });
        handle_ws_message(&state, &routes, &mut last_update_ts, trade).unwrap();
        let tob3 = state.get("516926").expect("tob should still exist");
        assert!(tob3.last_trade_ema.is_some());
        assert!(tob3.last_trade_ts.is_some());
        assert_eq!(
            tob3.ts, tob2.ts,
            "last trade should not overwrite book freshness ts"
        );
    }

    #[test]
    fn parse_side_levels_is_defensive_about_sorting() {
        // Unsorted bids (worst first): should still pick best=max.
        let bids = vec![
            WsPriceLevel {
                price: "0.001".to_string(),
                size: "5".to_string(),
            },
            WsPriceLevel {
                price: "0.490".to_string(),
                size: "10".to_string(),
            },
            WsPriceLevel {
                price: "0.480".to_string(),
                size: "7".to_string(),
            },
        ];
        let (best_bid, depth_bid_5) = parse_side_levels(&bids, true);
        assert_eq!(best_bid, Some(0.49));
        assert!(depth_bid_5 > 0.0);

        // Unsorted asks (worst first): should still pick best=min.
        let asks = vec![
            WsPriceLevel {
                price: "0.999".to_string(),
                size: "5".to_string(),
            },
            WsPriceLevel {
                price: "0.510".to_string(),
                size: "10".to_string(),
            },
            WsPriceLevel {
                price: "0.520".to_string(),
                size: "7".to_string(),
            },
        ];
        let (best_ask, depth_ask_5) = parse_side_levels(&asks, false);
        assert_eq!(best_ask, Some(0.51));
        assert!(depth_ask_5 > 0.0);
    }

    #[test]
    fn depth_is_top5_by_price_not_first5() {
        // If we naïvely took the first 5, depth would be 5.0. The correct answer includes the late best.
        let bids = vec![
            WsPriceLevel {
                price: "0.10".to_string(),
                size: "1".to_string(),
            },
            WsPriceLevel {
                price: "0.11".to_string(),
                size: "1".to_string(),
            },
            WsPriceLevel {
                price: "0.12".to_string(),
                size: "1".to_string(),
            },
            WsPriceLevel {
                price: "0.13".to_string(),
                size: "1".to_string(),
            },
            WsPriceLevel {
                price: "0.14".to_string(),
                size: "1".to_string(),
            },
            WsPriceLevel {
                price: "0.99".to_string(),
                size: "100".to_string(),
            }, // best bid, appears last
        ];
        let (best_bid, bid_depth_5) = parse_side_levels(&bids, true);
        assert_eq!(best_bid, Some(0.99));
        assert!(
            (bid_depth_5 - 104.0).abs() < 1e-12,
            "depth should be sum of top 5 by price"
        );

        let asks = vec![
            WsPriceLevel {
                price: "0.90".to_string(),
                size: "1".to_string(),
            },
            WsPriceLevel {
                price: "0.89".to_string(),
                size: "1".to_string(),
            },
            WsPriceLevel {
                price: "0.88".to_string(),
                size: "1".to_string(),
            },
            WsPriceLevel {
                price: "0.87".to_string(),
                size: "1".to_string(),
            },
            WsPriceLevel {
                price: "0.86".to_string(),
                size: "1".to_string(),
            },
            WsPriceLevel {
                price: "0.01".to_string(),
                size: "100".to_string(),
            }, // best ask (lowest), appears last
        ];
        let (best_ask, ask_depth_5) = parse_side_levels(&asks, false);
        assert_eq!(best_ask, Some(0.01));
        assert!(
            (ask_depth_5 - 104.0).abs() < 1e-12,
            "depth should be sum of top 5 by price"
        );
    }
}
