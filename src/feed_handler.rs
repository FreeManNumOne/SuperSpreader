 use std::collections::HashMap;
 
 use anyhow::Result;
 use parking_lot::RwLock;
 
 use crate::{
     config::Settings,
     market_selector::SelectedMarket,
     utils::{ewma, now_ts},
 };
 
 use polymarket_hft::client::polymarket::clob::Client as ClobClient;
 use polymarket_hft::client::polymarket::clob::orderbook::GetOrderBooksRequestItem;
 use polymarket_hft::client::polymarket::clob::Side as ClobSide;
 use polymarket_hft::client::polymarket::clob::ws::ClobWsClient;
 use polymarket_hft::client::polymarket::clob::ws::WsMessage;
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
         Self { inner: std::sync::Arc::new(RwLock::new(HashMap::new())) }
     }
 
     pub fn upsert(&self, market_id: &str, tob: Tob) {
         self.inner.write().insert(market_id.to_string(), tob);
     }
 
     pub fn get(&self, market_id: &str) -> Option<Tob> {
         self.inner.read().get(market_id).cloned()
     }
 
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
 
     pub fn spawn(self, selected_rx: watch::Receiver<Vec<SelectedMarket>>, store: crate::store::SqliteStore) {
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
                 let markets = selected_routes_rx.borrow().clone();
                 let mut r = routes.write();
                 r.by_condition.clear();
                 r.by_asset.clear();
                 r.token_for_market.clear();
 
                 for m in &markets {
                     if let Some(cid) = m.condition_id.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty()) {
                         r.by_condition.insert(cid.to_string(), m.market_id.clone());
                     }
                     if let Some(tid) = m.clob_token_id.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty()) {
                         r.by_asset.insert(tid.to_string(), m.market_id.clone());
                         r.token_for_market.insert(m.market_id.clone(), tid.to_string());
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
             if let Err(e) = run_ws_loop(settings_ws, state_ws, routes_ws, selected_ws_rx, store_ws).await {
                 log::error!("feed.ws.loop.error {}", e);
             }
         });
 
         // CLOB orderbook polling loop (freshness + safety net)
         let routes_poll = self.routes.clone();
         let state_poll = state.clone();
         let settings_poll = settings.clone();
         let store_poll = store.clone();
         tokio::spawn(async move {
             if let Err(e) = run_poll_loop(settings_poll, state_poll, routes_poll, selected_rx, store_poll).await {
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
     mut selected_rx: watch::Receiver<Vec<SelectedMarket>>,
     store: crate::store::SqliteStore,
 ) -> Result<()> {
     let mut ws = ClobWsClient::builder().base_url(ws_base_url(&settings.clob_ws_url)).build();
     let mut last_subscribed: Vec<String> = vec![];
 
     // Per-market last update for EWMA.
     let mut last_update_ts: HashMap<String, f64> = HashMap::new();
 
     loop {
         // Ensure subscription up to date.
         let desired = selected_rx.borrow().iter().filter_map(|m| m.clob_token_id.clone()).collect::<Vec<_>>();
         let mut desired_sorted = desired.clone();
         desired_sorted.sort();
         if desired_sorted != last_subscribed {
             last_subscribed = desired_sorted.clone();
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
         }
 
         // Poll for selection updates without blocking message reads forever.
         tokio::select! {
             biased;
             _ = selected_rx.changed() => {
                 continue;
             }
             msg = async { ws.next_message().await } => {
                 let Some(msg) = msg else {
                     // Connection dropped and auto-reconnect may have failed; wait for next tick.
                     tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                     continue;
                 };
                 handle_ws_message(&state, &routes, &mut last_update_ts, msg)?;
             }
         }
     }
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
             let (best_bid, bid_depth_5) = parse_side_levels(&b.bids);
             let (best_ask, ask_depth_5) = parse_side_levels(&b.asks);
 
             let prev = state.get(&mid);
             let prev_updates = prev.as_ref().map(|x| x.updates_ewma_per_min).unwrap_or(0.0);
             let prev_trade_ema = prev.as_ref().and_then(|x| x.last_trade_ema);
             let prev_trade_ts = prev.as_ref().and_then(|x| x.last_trade_ts);
 
             let inst_per_min = last_update_ts
                 .get(&mid)
                 .and_then(|prev_ts| {
                     let dt = (ts - *prev_ts).max(0.0);
                     if dt <= 0.0 { None } else { Some(60.0 / dt) }
                 })
                 .unwrap_or(0.0);
             last_update_ts.insert(mid.clone(), ts);
             let updates_ewma_per_min = ewma(Some(prev_updates), inst_per_min, 0.1);
 
             state.upsert(
                 &mid,
                 Tob {
                     best_bid,
                     best_ask,
                     bid_depth_5,
                     ask_depth_5,
                     ts,
                     updates_ewma_per_min,
                     last_trade_ema: prev_trade_ema,
                     last_trade_ts: prev_trade_ts,
                 },
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
             let Some(mid) = market_id else { return Ok(()); };
             let px = t.price.parse::<f64>().ok();
             let ts = parse_ws_ts(&t.timestamp).unwrap_or_else(now_ts);
             if let Some(p) = px {
                 let prev = state.get(&mid);
                 let prev_ema = prev.as_ref().and_then(|x| x.last_trade_ema);
                 let ema = ewma(prev_ema, p, 0.2);
 
                 let mut tob = prev.unwrap_or(Tob {
                     best_bid: None,
                     best_ask: None,
                     bid_depth_5: 0.0,
                     ask_depth_5: 0.0,
                     ts,
                     updates_ewma_per_min: 0.0,
                     last_trade_ema: None,
                     last_trade_ts: None,
                 });
                 tob.last_trade_ema = Some(ema);
                 tob.last_trade_ts = Some(ts);
                 // Do not overwrite tob.ts here; it's book freshness.
                 state.upsert(&mid, tob);
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
     mut selected_rx: watch::Receiver<Vec<SelectedMarket>>,
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
         let req = tokens
             .iter()
             .map(|t| GetOrderBooksRequestItem { token_id: t.clone(), side: Some(ClobSide::Buy) })
             .collect::<Vec<_>>();
 
         let books = match clob.get_order_books(&req).await {
             Ok(b) => b,
             Err(e) => {
                 store
                     .upsert_runtime_status("feed.poll", "error", "orderbook_poll_failed", Some(&e.to_string()), now_ts())
                     .ok();
                 continue;
             }
         };
 
         store
             .upsert_runtime_status("feed.poll", "ok", &format!("polled {}", books.len()), None, now_ts())
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
             let Some(market_id) = mid else { continue; };
 
             let (best_bid, bid_depth_5) = parse_side_levels_ob(&b.bids);
             let (best_ask, ask_depth_5) = parse_side_levels_ob(&b.asks);
 
             // Preserve book update EWMA and trade EMA, but refresh the timestamp.
             let prev = state.get(&market_id);
             let updates_ewma_per_min = prev.as_ref().map(|x| x.updates_ewma_per_min).unwrap_or(0.0);
             let last_trade_ema = prev.as_ref().and_then(|x| x.last_trade_ema);
             let last_trade_ts = prev.as_ref().and_then(|x| x.last_trade_ts);
 
             state.upsert(
                 &market_id,
                 Tob {
                     best_bid,
                     best_ask,
                     bid_depth_5,
                     ask_depth_5,
                     ts: now.max(parse_ws_ts(&b.timestamp).unwrap_or(now)),
                     updates_ewma_per_min,
                     last_trade_ema,
                     last_trade_ts,
                 },
             );
         }
     }
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
 
 fn parse_side_levels(levels: &[polymarket_hft::client::polymarket::clob::ws::WsPriceLevel]) -> (Option<f64>, f64) {
     let mut best: Option<f64> = None;
     let mut depth = 0.0;
     for (i, lvl) in levels.iter().enumerate() {
         let px = lvl.price.parse::<f64>().ok();
         let sz = lvl.size.parse::<f64>().ok();
         if i == 0 {
             best = px;
         }
         if i < 5 {
             depth += sz.unwrap_or(0.0);
         }
     }
     (best, depth)
 }
 
 fn parse_side_levels_ob(levels: &[polymarket_hft::client::polymarket::clob::orderbook::PriceLevel]) -> (Option<f64>, f64) {
     let mut best: Option<f64> = None;
     let mut depth = 0.0;
     for (i, lvl) in levels.iter().enumerate() {
         let px = lvl.price.parse::<f64>().ok();
         let sz = lvl.size.parse::<f64>().ok();
         if i == 0 {
             best = px;
         }
         if i < 5 {
             depth += sz.unwrap_or(0.0);
         }
     }
     (best, depth)
 }
