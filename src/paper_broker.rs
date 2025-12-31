 use std::collections::HashMap;
 
 use anyhow::Result;
 use rand::{rngs::SmallRng, Rng, SeedableRng};
 use serde::{Deserialize, Serialize};
 use serde_json::json;
 use uuid::Uuid;
 
 use crate::{
     config::Settings,
     feed_handler::Tob,
     store::SqliteStore,
     utils::{now_ts, poisson_sample},
 };
 
 #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
 pub enum Side {
     Buy,
     Sell,
 }
 
 impl Side {
     pub fn as_str(&self) -> &'static str {
         match self {
             Side::Buy => "buy",
             Side::Sell => "sell",
         }
     }
 }
 
 #[derive(Debug, Clone)]
 pub struct Order {
     pub order_id: String,
     pub market_id: String,
     pub side: Side,
     pub price: f64,
     pub size: f64,
     pub created_ts: f64,
     pub status: String, // open|cancelled|filled|rejected
     pub filled_size: f64,
     pub last_event_ts: f64,
     pub meta: serde_json::Value,
 }
 
 #[derive(Debug, Clone)]
 pub struct Fill {
     pub fill_id: String,
     pub order_id: String,
     pub market_id: String,
     pub side: Side,
     pub price: f64,
     pub size: f64,
     pub ts: f64,
 }
 
 #[derive(Debug, Clone, Default)]
 pub struct Position {
     pub qty: f64,
     pub avg_price: f64,
     pub realized_pnl: f64,
 }
 
 pub struct PaperBroker {
     settings: Settings,
     store: SqliteStore,
     rng: SmallRng,
     pub orders: HashMap<String, Order>,
     pub positions: HashMap<String, Position>, // by market_id
     last_sim_ts: HashMap<String, f64>,
     pub counters: BrokerCounters,
 }
 
 #[derive(Debug, Clone, Default)]
 pub struct BrokerCounters {
     pub orders_placed: u64,
     pub orders_cancelled: u64,
     pub fills: u64,
     pub filled_qty: f64,
     pub rejected_orders: u64,
     pub cancel_failures: u64,
 }
 
 impl PaperBroker {
     pub fn new(settings: Settings, store: SqliteStore) -> Self {
         Self {
             settings,
             store,
             rng: SmallRng::seed_from_u64(rand::random()),
             orders: HashMap::new(),
             positions: HashMap::new(),
             last_sim_ts: HashMap::new(),
             counters: BrokerCounters::default(),
         }
     }
 
     pub fn position_qty(&self, market_id: &str) -> f64 {
         self.positions.get(market_id).map(|p| p.qty).unwrap_or(0.0)
     }
 
     pub fn realized_pnl_total(&self) -> f64 {
         self.positions.values().map(|p| p.realized_pnl).sum()
     }
 
     pub fn mark_to_market(&self, market_id: &str, mark: f64) -> (f64, f64, f64) {
         let mut total_u = 0.0;
         let mut total_r = 0.0;
         let mut total = 0.0;
         if let Some(p) = self.positions.get(market_id) {
             let u = (mark - p.avg_price) * p.qty;
             total_u += u;
             total_r += p.realized_pnl;
             total += u + p.realized_pnl;
         }
         (total_u, total_r, total)
     }
 
     pub fn place_limit(
         &mut self,
         market_id: &str,
         side: Side,
         price: f64,
         size: f64,
         strategy: &str,
     ) -> Result<String> {
         let ts = now_ts();
         // Random "server faults" and non-atomic fails.
         if self.rng.random::<f64>() < self.settings.paper_fault_rate {
             self.counters.rejected_orders += 1;
             let oid = Uuid::new_v4().to_string();
             let o = Order {
                 order_id: oid.clone(),
                 market_id: market_id.to_string(),
                 side,
                 price,
                 size,
                 created_ts: ts,
                 status: "rejected".to_string(),
                 filled_size: 0.0,
                 last_event_ts: ts,
                 meta: json!({"strategy": strategy, "reason": "paper_fault"}),
             };
             self.store.insert_order(
                 &o.order_id,
                 &o.market_id,
                 o.side.as_str(),
                 o.price,
                 o.size,
                 o.created_ts,
                 &o.status,
                 o.filled_size,
                 &o.meta,
             )?;
             self.orders.insert(oid.clone(), o);
             return Ok(oid);
         }
 
         let oid = Uuid::new_v4().to_string();
         self.counters.orders_placed += 1;
         let o = Order {
             order_id: oid.clone(),
             market_id: market_id.to_string(),
             side,
             price,
             size,
             created_ts: ts,
             status: "open".to_string(),
             filled_size: 0.0,
             last_event_ts: ts,
             meta: json!({"strategy": strategy}),
         };
         self.store.insert_order(
             &o.order_id,
             &o.market_id,
             o.side.as_str(),
             o.price,
             o.size,
             o.created_ts,
             &o.status,
             o.filled_size,
             &o.meta,
         )?;
         self.orders.insert(oid.clone(), o);
 
         // Non-atomic fail: order appears open but can't be modified later. We store a flag.
         if self.rng.random::<f64>() < self.settings.paper_non_atomic_fail_rate {
             if let Some(x) = self.orders.get_mut(&oid) {
                 x.meta["non_atomic"] = json!(true);
                 self.store.insert_order(
                     &x.order_id,
                     &x.market_id,
                     x.side.as_str(),
                     x.price,
                     x.size,
                     x.created_ts,
                     &x.status,
                     x.filled_size,
                     &x.meta,
                 )?;
             }
         }
         Ok(oid)
     }
 
     pub fn cancel(&mut self, order_id: &str) -> Result<()> {
         let ts = now_ts();
         let Some(o) = self.orders.get_mut(order_id) else { return Ok(()); };
         if o.status != "open" {
             return Ok(());
         }
         if o.meta.get("non_atomic").and_then(|v| v.as_bool()) == Some(true) {
             // Simulate cancel failure.
             self.counters.cancel_failures += 1;
             o.last_event_ts = ts;
             o.meta["cancel_error"] = json!("non_atomic_fail");
             self.store.insert_order(
                 &o.order_id,
                 &o.market_id,
                 o.side.as_str(),
                 o.price,
                 o.size,
                 o.created_ts,
                 &o.status,
                 o.filled_size,
                 &o.meta,
             )?;
             return Ok(());
         }
         o.status = "cancelled".to_string();
         o.last_event_ts = ts;
         self.counters.orders_cancelled += 1;
         self.store.update_order_status(&o.order_id, &o.status, Some(o.filled_size))?;
         Ok(())
     }
 
     /// Execute an immediate fill ("IOC") against the current top-of-book.
     /// Used for snipe/arb behaviors in paper mode.
     pub fn execute_ioc(
         &mut self,
         market_id: &str,
         side: Side,
         price: f64,
         size: f64,
         strategy: &str,
         tob: &Tob,
     ) -> Result<Option<Fill>> {
        if self.settings.execution_mode == "shadow" {
            return Ok(None);
        }
         let ts = now_ts();
         let (Some(bid), Some(ask)) = (tob.best_bid, tob.best_ask) else { return Ok(None); };
         // Basic crossing checks.
         match side {
             Side::Buy if price < ask => return Ok(None),
             Side::Sell if price > bid => return Ok(None),
             _ => {}
         }
 
         let oid = Uuid::new_v4().to_string();
         let mut o = Order {
             order_id: oid.clone(),
             market_id: market_id.to_string(),
             side,
             price,
             size,
             created_ts: ts,
             status: "filled".to_string(),
             filled_size: size,
             last_event_ts: ts,
             meta: json!({"strategy": strategy, "type": "ioc"}),
         };
 
         let fill = Fill {
             fill_id: Uuid::new_v4().to_string(),
             order_id: oid.clone(),
             market_id: market_id.to_string(),
             side,
             price,
             size,
             ts,
         };
 
         self.apply_fill(&fill, tob)?;
         self.counters.fills += 1;
         self.counters.filled_qty += size;
 
         self.store.insert_order(
             &o.order_id,
             &o.market_id,
             o.side.as_str(),
             o.price,
             o.size,
             o.created_ts,
             &o.status,
             o.filled_size,
             &o.meta,
         )?;
         self.store.insert_fill(
             &fill.fill_id,
             &fill.order_id,
             &fill.market_id,
             fill.side.as_str(),
             fill.price,
             fill.size,
             fill.ts,
             &json!({"strategy": strategy, "type":"ioc"}),
         )?;
 
         o.meta["fill_id"] = json!(fill.fill_id);
         self.orders.insert(oid, o);
         Ok(Some(fill))
     }
 
     pub fn simulate_fills_for_market(&mut self, market_id: &str, tob: &Tob, activity_score: f64) -> Result<Vec<Fill>> {
        if self.settings.execution_mode == "shadow" {
            return Ok(vec![]);
        }
         let now = now_ts();
         let prev = self.last_sim_ts.get(market_id).copied().unwrap_or(now);
         let dt = (now - prev).max(0.0);
         self.last_sim_ts.insert(market_id.to_string(), now);
         if dt <= 0.0 {
             return Ok(vec![]);
         }
 
         let (Some(best_bid), Some(best_ask)) = (tob.best_bid, tob.best_ask) else {
             return Ok(vec![]);
         };
         if best_ask <= best_bid {
             return Ok(vec![]);
         }
 
         // We simulate passive maker fills driven by Poisson-arrival opponent orders.
         // Each resting order has a fill intensity based on distance-to-touch and activity.
         let tick = self.settings.price_tick.max(1e-6);
         let base_lambda = self.settings.paper_poisson_lambda_per_sec.max(0.0);
 
         let mut fills_out: Vec<Fill> = vec![];
         let order_ids: Vec<String> = self
             .orders
             .iter()
             .filter(|(_, o)| o.market_id == market_id && o.status == "open")
             .map(|(k, _)| k.clone())
             .collect();
 
         for oid in order_ids {
             // Compute fill proposal while holding a mutable borrow to the order only.
            let proposal: Option<(Fill, String, f64, serde_json::Value)> = (|| {
                let o = self.orders.get_mut(&oid)?;
                let rested = (now - o.created_ts).max(0.0);
                if rested < self.settings.paper_min_rest_secs {
                    return None;
                }
                let remaining = (o.size - o.filled_size).max(0.0);
                if remaining <= 0.0 {
                    return None;
                }
 
                 let distance_ticks = match o.side {
                     Side::Buy => ((best_bid - o.price) / tick).max(0.0),
                     Side::Sell => ((o.price - best_ask) / tick).max(0.0),
                 };
 
                 let at_touch_factor = (-0.7 * distance_ticks).exp();
                 let intensity = base_lambda * activity_score.max(0.05) * at_touch_factor;
                 let lambda_dt = intensity * dt;
 
                 let n = poisson_sample(&mut self.rng, lambda_dt);
                 if n == 0 {
                     return None;
                 }
 
                 let frac = (0.3 + 0.7 * self.rng.random::<f64>()).clamp(0.05, 1.0);
                 let fill_size = (remaining * frac).max(tick.min(remaining)).min(remaining);
 
                 o.filled_size += fill_size;
                 if o.filled_size + 1e-12 >= o.size {
                     o.status = "filled".to_string();
                 }
                 o.last_event_ts = now;
 
                 let fill = Fill {
                     fill_id: Uuid::new_v4().to_string(),
                     order_id: o.order_id.clone(),
                     market_id: o.market_id.clone(),
                     side: o.side,
                     price: o.price,
                     size: fill_size,
                     ts: now,
                 };
 
                 let status = o.status.clone();
                 let filled_size_total = o.filled_size;
                 let strat = o.meta.get("strategy").cloned().unwrap_or(json!("mm"));
                 Some((fill, status, filled_size_total, strat))
            })();
 
             let Some((fill, status, filled_size_total, strat)) = proposal else { continue; };
 
             self.apply_fill(&fill, tob)?;
             self.counters.fills += 1;
             self.counters.filled_qty += fill.size;
 
             self.store.update_order_status(&fill.order_id, &status, Some(filled_size_total))?;
             self.store.insert_fill(
                 &fill.fill_id,
                 &fill.order_id,
                 &fill.market_id,
                 fill.side.as_str(),
                 fill.price,
                 fill.size,
                 fill.ts,
                 &json!({"strategy": strat, "fill_model": "maker_touch"}),
             )?;
             fills_out.push(fill);
         }
 
         Ok(fills_out)
     }
 
     fn apply_fill(&mut self, fill: &Fill, tob: &Tob) -> Result<()> {
         let pos = self.positions.entry(fill.market_id.clone()).or_default();
 
         // Execution costs (fees=0, but slippage+latency modeled as a per-fill penalty).
         let cost_bps = (self.settings.slippage_bps + self.settings.latency_bps).max(0.0);
         let exec_cost = (cost_bps / 10_000.0) * fill.price * fill.size;
         pos.realized_pnl -= exec_cost;
 
         match fill.side {
             Side::Buy => {
                 if pos.qty >= 0.0 {
                     // Add to / open long.
                     let new_qty = pos.qty + fill.size;
                     pos.avg_price = if new_qty.abs() > 1e-12 {
                         (pos.avg_price * pos.qty + fill.price * fill.size) / new_qty
                     } else {
                         0.0
                     };
                     pos.qty = new_qty;
                 } else {
                     // Reduce short first.
                     let short = -pos.qty;
                     let close_qty = short.min(fill.size);
                     // Short PnL: sold at avg, buy back at fill.price.
                     pos.realized_pnl += (pos.avg_price - fill.price) * close_qty;
                     let rem = fill.size - close_qty;
                     pos.qty += close_qty; // qty is negative; adding close reduces magnitude
                     if rem > 0.0 {
                         // Flip to long for remaining.
                         pos.qty += rem;
                         pos.avg_price = fill.price;
                     }
                     if pos.qty.abs() < 1e-12 {
                         pos.qty = 0.0;
                         pos.avg_price = 0.0;
                     }
                 }
             }
             Side::Sell => {
                 if pos.qty <= 0.0 {
                     // Add to / open short.
                     let new_short = (-pos.qty) + fill.size;
                     let new_qty = -new_short;
                     pos.avg_price = if new_short.abs() > 1e-12 {
                         (pos.avg_price * (-pos.qty) + fill.price * fill.size) / new_short
                     } else {
                         0.0
                     };
                     pos.qty = new_qty;
                 } else {
                     // Reduce long first.
                     let long = pos.qty;
                     let close_qty = long.min(fill.size);
                     pos.realized_pnl += (fill.price - pos.avg_price) * close_qty;
                     let rem = fill.size - close_qty;
                     pos.qty -= close_qty;
                     if rem > 0.0 {
                         // Flip to short.
                         pos.qty -= rem;
                         pos.avg_price = fill.price;
                     }
                     if pos.qty.abs() < 1e-12 {
                         pos.qty = 0.0;
                         pos.avg_price = 0.0;
                     }
                 }
             }
         }
 
         // Keep mark in meta? (snapshots handle mark separately).
         let _ = tob;
         Ok(())
     }
 }
 
