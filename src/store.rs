 use std::path::Path;
 
 use anyhow::{Context, Result};
 use rusqlite::{params, Connection, OptionalExtension};
 use serde_json::Value as JsonValue;
 
 #[derive(Clone)]
 pub struct SqliteStore {
     path: String,
 }
 
 impl SqliteStore {
     pub fn new(path: &str) -> Result<Self> {
         if path.trim().is_empty() {
             anyhow::bail!("SQLITE_PATH is empty");
         }
         if path != ":memory:" && !path.starts_with("file:") {
             if let Some(parent) = Path::new(path).parent() {
                 std::fs::create_dir_all(parent)
                     .with_context(|| format!("create sqlite parent dir for {path}"))?;
             }
         }
 
         // Note: rusqlite::Connection is not Send/Sync. We keep only a path here,
         // and open short-lived connections per operation. WAL keeps this fast enough
         // for the dashboard and light telemetry writes.
         //
         // For tight HFT loops, the bot uses an internal batching layer (see bot module)
         // to reduce write frequency.
         Ok(Self { path: path.to_string() })
     }
 
     pub fn path(&self) -> &str {
         &self.path
     }
 
     fn open_conn(&self) -> Result<Connection> {
         let conn = Connection::open(&self.path).with_context(|| format!("open sqlite {}", self.path))?;
         conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
         Ok(conn)
     }
 
     pub fn init_db(&self) -> Result<()> {
         let conn = self.open_conn()?;
         conn.execute_batch(
             r#"
 CREATE TABLE IF NOT EXISTS markets (
   market_id TEXT PRIMARY KEY,
   question TEXT,
   event_id TEXT,
   active INTEGER,
   end_ts REAL,
   volume_24h_usd REAL,
   liquidity_usd REAL,
   condition_id TEXT,
   clob_token_id TEXT,
   updated_ts REAL
 );
 
 CREATE TABLE IF NOT EXISTS orders (
   order_id TEXT PRIMARY KEY,
   market_id TEXT,
   side TEXT,
   price REAL,
   size REAL,
   created_ts REAL,
   status TEXT,
   filled_size REAL,
   meta_json TEXT
 );
 
 CREATE TABLE IF NOT EXISTS fills (
   fill_id TEXT PRIMARY KEY,
   order_id TEXT,
   market_id TEXT,
   side TEXT,
   price REAL,
   size REAL,
   ts REAL,
   meta_json TEXT
 );
 
 CREATE TABLE IF NOT EXISTS tape (
   id INTEGER PRIMARY KEY AUTOINCREMENT,
   ts REAL,
   market_id TEXT,
   kind TEXT,
   payload_json TEXT
 );
 
 CREATE INDEX IF NOT EXISTS idx_tape_ts ON tape(ts);
 CREATE INDEX IF NOT EXISTS idx_tape_market ON tape(market_id, ts);
 
 CREATE TABLE IF NOT EXISTS position_snapshots (
   id INTEGER PRIMARY KEY AUTOINCREMENT,
   ts REAL,
   market_id TEXT,
   event_id TEXT,
   position REAL,
   avg_price REAL,
   mark_price REAL,
   unrealized_pnl REAL,
   realized_pnl REAL
 );
 
 CREATE TABLE IF NOT EXISTS pnl_snapshots (
   id INTEGER PRIMARY KEY AUTOINCREMENT,
   ts REAL,
   total_unrealized REAL,
   total_realized REAL,
   total_pnl REAL
 );
 
 CREATE TABLE IF NOT EXISTS quote_snapshots (
   id INTEGER PRIMARY KEY AUTOINCREMENT,
   ts REAL,
   market_id TEXT,
   event_id TEXT,
   tob_best_bid REAL,
   tob_best_ask REAL,
   mid REAL,
   fair REAL,
   fair_source TEXT,
   inv_qty REAL,
   width REAL,
   skew REAL,
   target_bid REAL,
   target_ask REAL
 );
 
 CREATE INDEX IF NOT EXISTS idx_quotes_ts ON quote_snapshots(ts);
 CREATE INDEX IF NOT EXISTS idx_quotes_market ON quote_snapshots(market_id, ts);
 
 CREATE TABLE IF NOT EXISTS scanner_snapshots (
   id INTEGER PRIMARY KEY AUTOINCREMENT,
   ts REAL,
   eligible_count INTEGER,
   top_count INTEGER
 );
 
 CREATE INDEX IF NOT EXISTS idx_scanner_ts ON scanner_snapshots(ts);
 
 CREATE TABLE IF NOT EXISTS watchlist (
   rank INTEGER PRIMARY KEY,
   market_id TEXT,
   ts REAL
 );
 
 CREATE TABLE IF NOT EXISTS runtime_status (
   component TEXT PRIMARY KEY,
   ts REAL,
   level TEXT,
   message TEXT,
   detail TEXT
 );
 "#,
         )?;
         Ok(())
     }
 
     pub fn clear_trading_state(&self) -> Result<()> {
         let conn = self.open_conn()?;
         conn.execute_batch(
             r#"
 DELETE FROM orders;
 DELETE FROM fills;
 DELETE FROM position_snapshots;
 DELETE FROM pnl_snapshots;
 "#,
         )?;
         Ok(())
     }
 
     pub fn upsert_runtime_status(
         &self,
         component: &str,
         level: &str,
         message: &str,
         detail: Option<&str>,
         ts: f64,
     ) -> Result<()> {
         let conn = self.open_conn()?;
         conn.execute(
             r#"
 INSERT INTO runtime_status(component, ts, level, message, detail)
 VALUES(?,?,?,?,?)
 ON CONFLICT(component) DO UPDATE SET
   ts=excluded.ts,
   level=excluded.level,
   message=excluded.message,
   detail=excluded.detail
 "#,
             params![component, ts, level, message, detail],
         )?;
         Ok(())
     }
 
     pub fn insert_scanner_snapshot(&self, ts: f64, eligible_count: i64, top_count: i64) -> Result<()> {
         let conn = self.open_conn()?;
         conn.execute(
             "INSERT INTO scanner_snapshots(ts, eligible_count, top_count) VALUES(?,?,?)",
             params![ts, eligible_count, top_count],
         )?;
         Ok(())
     }
 
     pub fn update_watchlist(&self, market_ids: &[String], ts: f64) -> Result<()> {
         let mut conn = self.open_conn()?;
         let tx = conn.transaction()?;
         tx.execute("DELETE FROM watchlist", [])?;
         {
             let mut stmt = tx.prepare("INSERT INTO watchlist(rank, market_id, ts) VALUES(?,?,?)")?;
             for (i, mid) in market_ids.iter().enumerate() {
                 stmt.execute(params![i as i64 + 1, mid, ts])?;
             }
         }
         tx.commit()?;
         Ok(())
     }
 
     pub fn upsert_market(
         &self,
         market_id: &str,
         question: Option<&str>,
         event_id: Option<&str>,
         active: bool,
         end_ts: Option<f64>,
         volume_24h_usd: f64,
         liquidity_usd: f64,
         condition_id: Option<&str>,
         clob_token_id: Option<&str>,
         updated_ts: f64,
     ) -> Result<()> {
         let conn = self.open_conn()?;
         conn.execute(
             r#"
 INSERT INTO markets(
   market_id, question, event_id, active, end_ts,
   volume_24h_usd, liquidity_usd, condition_id, clob_token_id, updated_ts
 )
 VALUES(?,?,?,?,?,?,?,?,?,?)
 ON CONFLICT(market_id) DO UPDATE SET
   question=excluded.question,
   event_id=excluded.event_id,
   active=excluded.active,
   end_ts=excluded.end_ts,
   volume_24h_usd=excluded.volume_24h_usd,
   liquidity_usd=excluded.liquidity_usd,
   condition_id=excluded.condition_id,
   clob_token_id=excluded.clob_token_id,
   updated_ts=excluded.updated_ts
 "#,
             params![
                 market_id,
                 question,
                 event_id,
                 if active { 1 } else { 0 },
                 end_ts,
                 volume_24h_usd,
                 liquidity_usd,
                 condition_id,
                 clob_token_id,
                 updated_ts
             ],
         )?;
         Ok(())
     }
 
     pub fn insert_order(
         &self,
         order_id: &str,
         market_id: &str,
         side: &str,
         price: f64,
         size: f64,
         created_ts: f64,
         status: &str,
         filled_size: f64,
         meta: &JsonValue,
     ) -> Result<()> {
         let conn = self.open_conn()?;
         conn.execute(
             r#"
 INSERT OR REPLACE INTO orders(order_id, market_id, side, price, size, created_ts, status, filled_size, meta_json)
 VALUES(?,?,?,?,?,?,?,?,?)
 "#,
             params![
                 order_id,
                 market_id,
                 side,
                 price,
                 size,
                 created_ts,
                 status,
                 filled_size,
                 serde_json::to_string(meta)?
             ],
         )?;
         Ok(())
     }
 
     pub fn update_order_status(&self, order_id: &str, status: &str, filled_size: Option<f64>) -> Result<()> {
         let conn = self.open_conn()?;
         match filled_size {
             None => {
                 conn.execute("UPDATE orders SET status=? WHERE order_id=?", params![status, order_id])?;
             }
             Some(fs) => {
                 conn.execute(
                     "UPDATE orders SET status=?, filled_size=? WHERE order_id=?",
                     params![status, fs, order_id],
                 )?;
             }
         }
         Ok(())
     }
 
     pub fn insert_fill(
         &self,
         fill_id: &str,
         order_id: &str,
         market_id: &str,
         side: &str,
         price: f64,
         size: f64,
         ts: f64,
         meta: &JsonValue,
     ) -> Result<()> {
         let conn = self.open_conn()?;
         conn.execute(
             r#"
 INSERT OR REPLACE INTO fills(fill_id, order_id, market_id, side, price, size, ts, meta_json)
 VALUES(?,?,?,?,?,?,?,?)
 "#,
             params![
                 fill_id,
                 order_id,
                 market_id,
                 side,
                 price,
                 size,
                 ts,
                 serde_json::to_string(meta)?
             ],
         )?;
         Ok(())
     }
 
     pub fn insert_quote_snapshot(
         &self,
         ts: f64,
         market_id: &str,
         event_id: &str,
         tob_best_bid: Option<f64>,
         tob_best_ask: Option<f64>,
         mid: Option<f64>,
         fair: Option<f64>,
         fair_source: &str,
         inv_qty: f64,
         width: f64,
         skew: f64,
         target_bid: Option<f64>,
         target_ask: Option<f64>,
     ) -> Result<()> {
         let conn = self.open_conn()?;
         conn.execute(
             r#"
 INSERT INTO quote_snapshots(
   ts, market_id, event_id,
   tob_best_bid, tob_best_ask,
   mid, fair, fair_source,
   inv_qty, width, skew,
   target_bid, target_ask
 )
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
 "#,
             params![
                 ts,
                 market_id,
                 event_id,
                 tob_best_bid,
                 tob_best_ask,
                 mid,
                 fair,
                 fair_source,
                 inv_qty,
                 width,
                 skew,
                 target_bid,
                 target_ask
             ],
         )?;
         Ok(())
     }
 
     pub fn insert_position_snapshot(
         &self,
         ts: f64,
         market_id: &str,
         event_id: &str,
         position: f64,
         avg_price: f64,
         mark_price: f64,
         unrealized_pnl: f64,
         realized_pnl: f64,
     ) -> Result<()> {
         let conn = self.open_conn()?;
         conn.execute(
             r#"
 INSERT INTO position_snapshots(ts, market_id, event_id, position, avg_price, mark_price, unrealized_pnl, realized_pnl)
 VALUES(?,?,?,?,?,?,?,?)
 "#,
             params![
                 ts, market_id, event_id, position, avg_price, mark_price, unrealized_pnl, realized_pnl
             ],
         )?;
         Ok(())
     }
 
     pub fn insert_pnl_snapshot(&self, ts: f64, total_unrealized: f64, total_realized: f64, total_pnl: f64) -> Result<()> {
         let conn = self.open_conn()?;
         conn.execute(
             "INSERT INTO pnl_snapshots(ts, total_unrealized, total_realized, total_pnl) VALUES(?,?,?,?)",
             params![ts, total_unrealized, total_realized, total_pnl],
         )?;
         Ok(())
     }
 
     // ---- Dashboard queries (read-only) ----
 
     pub fn fetch_latest_pnl(&self) -> Result<Option<JsonValue>> {
         let conn = self.open_conn()?;
         let row = conn
             .query_row(
                 "SELECT ts, total_unrealized, total_realized, total_pnl FROM pnl_snapshots ORDER BY ts DESC LIMIT 1",
                 [],
                 |r| {
                     Ok(serde_json::json!({
                         "ts": r.get::<_, f64>(0)?,
                         "total_unrealized": r.get::<_, f64>(1)?,
                         "total_realized": r.get::<_, f64>(2)?,
                         "total_pnl": r.get::<_, f64>(3)?,
                     }))
                 },
             )
             .optional()?;
         Ok(row)
     }
 
     pub fn fetch_latest_scanner_snapshot(&self) -> Result<Option<JsonValue>> {
         let conn = self.open_conn()?;
         let row = conn
             .query_row(
                 "SELECT ts, eligible_count, top_count FROM scanner_snapshots ORDER BY ts DESC LIMIT 1",
                 [],
                 |r| {
                     Ok(serde_json::json!({
                         "ts": r.get::<_, f64>(0)?,
                         "eligible_count": r.get::<_, i64>(1)?,
                         "top_count": r.get::<_, i64>(2)?,
                     }))
                 },
             )
             .optional()?;
         Ok(row)
     }
 
     pub fn fetch_latest_tape_ts(&self) -> Result<Option<f64>> {
         let conn = self.open_conn()?;
         let v: Option<f64> = conn
             .query_row("SELECT MAX(ts) FROM tape", [], |r| r.get(0))
             .optional()?
             .flatten();
         Ok(v)
     }
 
     pub fn fetch_latest_market_update_ts(&self) -> Result<Option<f64>> {
         let conn = self.open_conn()?;
         let v: Option<f64> = conn
             .query_row("SELECT MAX(updated_ts) FROM markets", [], |r| r.get(0))
             .optional()?
             .flatten();
         Ok(v)
     }
 
     pub fn fetch_runtime_statuses(&self) -> Result<JsonValue> {
         let conn = self.open_conn()?;
         let mut stmt = conn.prepare("SELECT component, ts, level, message, detail FROM runtime_status ORDER BY ts DESC")?;
         let mut rows = stmt.query([])?;
         let mut out = serde_json::Map::new();
         while let Some(r) = rows.next()? {
             let component: String = r.get(0)?;
             let ts: f64 = r.get(1)?;
             let level: String = r.get(2)?;
             let message: String = r.get(3)?;
             let detail: Option<String> = r.get(4)?;
             out.insert(
                 component.clone(),
                 serde_json::json!({
                     "component": component,
                     "ts": ts,
                     "level": level,
                     "message": message,
                     "detail": detail.unwrap_or_default(),
                 }),
             );
         }
         Ok(JsonValue::Object(out))
     }
 
     pub fn fetch_watchlist(&self, limit: usize) -> Result<Vec<JsonValue>> {
         let conn = self.open_conn()?;
         let mut stmt = conn.prepare(
             r#"
 SELECT w.rank, w.market_id, w.ts as watch_ts,
        m.question, m.event_id, m.active, m.end_ts, m.volume_24h_usd, m.liquidity_usd, m.updated_ts
 FROM watchlist w
 LEFT JOIN markets m ON m.market_id = w.market_id
 ORDER BY w.rank ASC
 LIMIT ?
 "#,
         )?;
         let mut rows = stmt.query(params![limit as i64])?;
         let mut out = vec![];
         while let Some(r) = rows.next()? {
             out.push(serde_json::json!({
                 "rank": r.get::<_, i64>(0)?,
                 "market_id": r.get::<_, String>(1)?,
                 "watch_ts": r.get::<_, f64>(2)?,
                 "question": r.get::<_, Option<String>>(3)?,
                 "event_id": r.get::<_, Option<String>>(4)?,
                 "active": r.get::<_, Option<i64>>(5)?.unwrap_or(1),
                 "end_ts": r.get::<_, Option<f64>>(6)?,
                 "volume_24h_usd": r.get::<_, Option<f64>>(7)?,
                 "liquidity_usd": r.get::<_, Option<f64>>(8)?,
                 "updated_ts": r.get::<_, Option<f64>>(9)?,
             }));
         }
         Ok(out)
     }
 
     pub fn fetch_latest_positions(&self, limit: usize) -> Result<Vec<JsonValue>> {
         let conn = self.open_conn()?;
         let mut stmt = conn.prepare(
             r#"
 WITH latest AS (
   SELECT market_id, MAX(id) AS id_max
   FROM position_snapshots
   GROUP BY market_id
 )
 SELECT ps.market_id,
        ps.event_id,
        ps.position,
        ps.avg_price,
        ps.mark_price,
        ps.unrealized_pnl,
        ps.realized_pnl,
        ps.ts
 FROM position_snapshots ps
 JOIN latest ON latest.id_max = ps.id
 ORDER BY ps.ts DESC
 LIMIT ?
 "#,
         )?;
         let mut rows = stmt.query(params![limit as i64])?;
         let mut out = vec![];
         while let Some(r) = rows.next()? {
             out.push(serde_json::json!({
                 "market_id": r.get::<_, String>(0)?,
                 "event_id": r.get::<_, String>(1)?,
                 "position": r.get::<_, f64>(2)?,
                 "avg_price": r.get::<_, f64>(3)?,
                 "mark_price": r.get::<_, f64>(4)?,
                 "unrealized_pnl": r.get::<_, f64>(5)?,
                 "realized_pnl": r.get::<_, f64>(6)?,
                 "ts": r.get::<_, f64>(7)?,
             }));
         }
         Ok(out)
     }
 
     pub fn fetch_latest_quotes(&self, limit: usize) -> Result<Vec<JsonValue>> {
         let conn = self.open_conn()?;
         let mut stmt = conn.prepare(
             r#"
 WITH latest AS (
   SELECT market_id, MAX(id) AS id_max
   FROM quote_snapshots
   GROUP BY market_id
 )
 SELECT q.market_id,
        q.event_id,
        q.tob_best_bid,
        q.tob_best_ask,
        q.mid,
        q.fair,
        q.fair_source,
        q.inv_qty,
        q.width,
        q.skew,
        q.target_bid,
        q.target_ask,
        q.ts,
        m.question
 FROM quote_snapshots q
 JOIN latest ON latest.id_max = q.id
 LEFT JOIN markets m ON m.market_id = q.market_id
 ORDER BY q.ts DESC
 LIMIT ?
 "#,
         )?;
         let mut rows = stmt.query(params![limit as i64])?;
         let mut out = vec![];
         while let Some(r) = rows.next()? {
             out.push(serde_json::json!({
                 "market_id": r.get::<_, String>(0)?,
                 "event_id": r.get::<_, String>(1)?,
                 "tob_best_bid": r.get::<_, Option<f64>>(2)?,
                 "tob_best_ask": r.get::<_, Option<f64>>(3)?,
                 "mid": r.get::<_, Option<f64>>(4)?,
                 "fair": r.get::<_, Option<f64>>(5)?,
                 "fair_source": r.get::<_, Option<String>>(6)?,
                 "inv_qty": r.get::<_, Option<f64>>(7)?,
                 "width": r.get::<_, Option<f64>>(8)?,
                 "skew": r.get::<_, Option<f64>>(9)?,
                 "target_bid": r.get::<_, Option<f64>>(10)?,
                 "target_ask": r.get::<_, Option<f64>>(11)?,
                 "ts": r.get::<_, f64>(12)?,
                 "question": r.get::<_, Option<String>>(13)?,
             }));
         }
         Ok(out)
     }
 
     pub fn fetch_recent_orders(&self, limit: usize, status: Option<&str>) -> Result<Vec<JsonValue>> {
         let conn = self.open_conn()?;
         let (sql, params_vec): (&str, Vec<rusqlite::types::Value>) = match status {
             None => (
                 r#"
 SELECT order_id, market_id, side, price, size, created_ts, status, filled_size, meta_json
 FROM orders
 ORDER BY created_ts DESC
 LIMIT ?
 "#,
                 vec![rusqlite::types::Value::Integer(limit as i64)],
             ),
             Some(st) => (
                 r#"
 SELECT order_id, market_id, side, price, size, created_ts, status, filled_size, meta_json
 FROM orders
 WHERE status = ?
 ORDER BY created_ts DESC
 LIMIT ?
 "#,
                 vec![
                     rusqlite::types::Value::Text(st.to_string()),
                     rusqlite::types::Value::Integer(limit as i64),
                 ],
             ),
         };
 
         let mut stmt = conn.prepare(sql)?;
         let mut rows = stmt.query(rusqlite::params_from_iter(params_vec))?;
         let mut out = vec![];
         while let Some(r) = rows.next()? {
             let meta_json: String = r.get(8)?;
             let meta: JsonValue = serde_json::from_str(&meta_json).unwrap_or_else(|_| serde_json::json!({}));
             out.push(serde_json::json!({
                 "order_id": r.get::<_, String>(0)?,
                 "market_id": r.get::<_, String>(1)?,
                 "side": r.get::<_, String>(2)?,
                 "price": r.get::<_, f64>(3)?,
                 "size": r.get::<_, f64>(4)?,
                 "created_ts": r.get::<_, f64>(5)?,
                 "status": r.get::<_, String>(6)?,
                 "filled_size": r.get::<_, f64>(7)?,
                 "meta": meta,
             }));
         }
         Ok(out)
     }
 
     pub fn fetch_recent_fills(&self, limit: usize) -> Result<Vec<JsonValue>> {
         let conn = self.open_conn()?;
         let mut stmt = conn.prepare(
             r#"
 SELECT fill_id, order_id, market_id, side, price, size, ts, meta_json
 FROM fills
 ORDER BY ts DESC
 LIMIT ?
 "#,
         )?;
         let mut rows = stmt.query(params![limit as i64])?;
         let mut out = vec![];
         while let Some(r) = rows.next()? {
             let meta_json: String = r.get(7)?;
             let meta: JsonValue = serde_json::from_str(&meta_json).unwrap_or_else(|_| serde_json::json!({}));
             out.push(serde_json::json!({
                 "fill_id": r.get::<_, String>(0)?,
                 "order_id": r.get::<_, String>(1)?,
                 "market_id": r.get::<_, String>(2)?,
                 "side": r.get::<_, String>(3)?,
                 "price": r.get::<_, f64>(4)?,
                 "size": r.get::<_, f64>(5)?,
                 "ts": r.get::<_, f64>(6)?,
                 "meta": meta,
             }));
         }
         Ok(out)
     }
 }
