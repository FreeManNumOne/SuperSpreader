 use crate::{
     config::Settings,
     feed_handler::Tob,
     paper_broker::{Side},
     utils::{clamp, round_to_tick},
 };
 
 #[derive(Debug, Clone)]
 pub struct QuoteIntent {
     pub side: Side,
     pub price: f64,
     pub size: f64,
 }
 
 pub struct HftStrategy {
     settings: Settings,
 }
 
 impl HftStrategy {
     pub fn new(settings: Settings) -> Self {
         Self { settings }
     }
 
     pub fn compute_fair(&self, tob: &Tob, ema_last_trade: Option<f64>) -> Option<(f64, &'static str)> {
         let mid = tob.mid()?;
         let fair = match ema_last_trade {
             Some(x) if x > 0.0 => 0.7 * mid + 0.3 * x,
             _ => mid,
         };
         Some((clamp(fair, self.settings.price_tick, 1.0 - self.settings.price_tick), "book_mid"))
     }
 
     pub fn quote_grid(
         &self,
         fair: f64,
         inv_qty: f64,
         imbalance: f64,
         activity_per_min: f64,
     ) -> Vec<QuoteIntent> {
         // Grid: 5-10 levels, tighter on higher activity.
         let levels = self.settings.mm_levels.clamp(5, 10);
         let tight = clamp(activity_per_min / 30.0, 0.0, 1.0);
         let base_width = clamp(0.01 - 0.005 * tight, 0.005, 0.01);
 
         // Inventory skew: linear, capped.
         let inv_ratio = clamp(inv_qty / self.settings.max_inventory_usd, -1.0, 1.0);
         let inv_skew = clamp(-inv_ratio * self.settings.inventory_skew_cap, -self.settings.inventory_skew_cap, self.settings.inventory_skew_cap);
 
         // Imbalance skew: small; bid-heavy -> skew up slightly (widen asks / lift bids)
         let imb_skew = clamp(imbalance * 0.0015, -0.0015, 0.0015);
 
         let skew = clamp(inv_skew + imb_skew, -self.settings.inventory_skew_cap, self.settings.inventory_skew_cap);
 
         let mut out = Vec::with_capacity(levels * 2);
         for i in 0..levels {
             let k = i as f64 + 1.0;
             let step = base_width * k;
             let bid = round_to_tick(clamp(fair - step + skew, self.settings.price_tick, 1.0 - self.settings.price_tick), self.settings.price_tick);
             let ask = round_to_tick(clamp(fair + step + skew, self.settings.price_tick, 1.0 - self.settings.price_tick), self.settings.price_tick);
             // keep non-crossing
             if bid < ask {
                 out.push(QuoteIntent { side: Side::Buy, price: bid, size: self.settings.base_order_size });
                 out.push(QuoteIntent { side: Side::Sell, price: ask, size: self.settings.base_order_size });
             }
         }
         out
     }
 }
