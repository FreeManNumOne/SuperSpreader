 use rand::Rng;
 use rand_distr::{Distribution, Poisson};
 
 pub fn now_ts() -> f64 {
     let now = std::time::SystemTime::now()
         .duration_since(std::time::UNIX_EPOCH)
         .unwrap_or_default();
     now.as_secs_f64()
 }
 
 pub fn clamp(x: f64, lo: f64, hi: f64) -> f64 {
     x.max(lo).min(hi)
 }
 
 pub fn round_to_tick(price: f64, tick: f64) -> f64 {
     if tick <= 0.0 {
         return price;
     }
     (price / tick).round() * tick
 }
 
 pub fn ewma(prev: Option<f64>, x: f64, alpha: f64) -> f64 {
     match prev {
         None => x,
         Some(p) => alpha * x + (1.0 - alpha) * p,
     }
 }
 
 pub fn poisson_sample(rng: &mut impl Rng, lambda: f64) -> u64 {
     if !(lambda > 0.0) {
         return 0;
     }
     let d = Poisson::new(lambda.max(0.0)).unwrap();
     d.sample(rng) as u64
 }
