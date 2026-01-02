use std::net::SocketAddr;

use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::{config::Settings, store::SqliteStore};

#[derive(Clone)]
pub struct DashboardState {
    pub settings: Settings,
    pub store: SqliteStore,
}

pub async fn serve_dashboard(settings: Settings, store: SqliteStore) -> Result<()> {
    let state = DashboardState {
        settings: settings.clone(),
        store,
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/api/summary", get(api_summary))
        .route("/api/health", get(api_health))
        .route("/api/watchlist", get(api_watchlist))
        .route("/api/positions", get(api_positions))
        .route("/api/orders", get(api_orders))
        .route("/api/open_orders", get(api_open_orders))
        .route("/api/quotes", get(api_quotes))
        .route("/api/fills", get(api_fills))
        .route("/api/publishers", get(api_publishers))
        .route("/api/admin/reset_paper_state", post(api_reset_paper_state))
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", settings.dashboard_host, settings.dashboard_port)
        .parse()
        .expect("dashboard addr parse");

    log::info!("dashboard.start url=http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index(State(st): State<DashboardState>) -> impl IntoResponse {
    let host = st.settings.dashboard_host.clone();
    let port = st.settings.dashboard_port;
    let mode = st.settings.run_mode.clone();
    let trade_mode = st.settings.trade_mode.clone();
    let sqlite_path = st.store.path().to_string();
    let can_reset = trade_mode == "paper" && st.settings.dashboard_enable_reset;
    Html(render_index_html(
        &host,
        port,
        &mode,
        &trade_mode,
        &sqlite_path,
        can_reset,
    ))
}

fn render_index_html(
    host: &str,
    port: u16,
    mode: &str,
    trade_mode: &str,
    sqlite_path: &str,
    can_reset: bool,
) -> String {
    // This is intentionally kept as a single-file UI (no build step),
    // lifted from the existing Python dashboard so you can keep the same look & feel.
    let reset_btn = if can_reset {
        r#"<button class="btn" id="resetBtn" style="border-color: rgba(255,77,77,0.45);">Reset paper state</button>"#
    } else {
        ""
    };

    format!(
        r#"<!doctype html>
 <html lang="en">
   <head>
     <meta charset="utf-8" />
     <meta name="viewport" content="width=device-width, initial-scale=1" />
     <title>SuperSpreader • Trading Dashboard</title>
     <style>
       :root {{
         --bg: #0b1220;
         --panel: rgba(255,255,255,0.06);
         --panel2: rgba(255,255,255,0.08);
         --stroke: rgba(255,255,255,0.12);
         --text: rgba(255,255,255,0.92);
         --muted: rgba(255,255,255,0.65);
         --good: #33d17a;
         --bad: #ff4d4d;
         --warn: #ffcc00;
         --brand: #7c5cff;
         --brand2: #3dd6d0;
       }}
       * {{ box-sizing: border-box; }}
       body {{
         margin: 0;
         font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
         color: var(--text);
         background: radial-gradient(1200px 900px at 15% 10%, rgba(124,92,255,0.20), transparent 60%),
                     radial-gradient(1100px 800px at 90% 20%, rgba(61,214,208,0.16), transparent 55%),
                     radial-gradient(900px 700px at 30% 90%, rgba(255,77,77,0.10), transparent 55%),
                     var(--bg);
       }}
       .wrap {{ max-width: 1280px; margin: 0 auto; padding: 22px 18px 42px; }}
       .topbar {{
         display: flex; align-items: center; justify-content: space-between; gap: 12px;
         padding: 16px 16px; border: 1px solid var(--stroke); border-radius: 16px;
         background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03));
         backdrop-filter: blur(10px);
       }}
       .brand {{ display: flex; align-items: center; gap: 12px; }}
       .logo {{
         width: 42px; height: 42px; border-radius: 12px;
         background: conic-gradient(from 180deg, var(--brand), var(--brand2), var(--brand));
         box-shadow: 0 12px 30px rgba(124,92,255,0.25);
       }}
       .title {{ font-weight: 800; letter-spacing: 0.2px; }}
       .subtitle {{ color: var(--muted); font-size: 12px; margin-top: 2px; }}
       .chips {{ display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; }}
       .chip {{
         padding: 7px 10px; border-radius: 999px; border: 1px solid var(--stroke);
         background: rgba(255,255,255,0.04);
         font-size: 12px; color: var(--muted);
         white-space: nowrap;
       }}
       .chip b {{ color: var(--text); font-weight: 700; }}
       .grid {{ display: grid; gap: 14px; margin-top: 14px; grid-template-columns: repeat(12, 1fr); }}
       .card {{
         border: 1px solid var(--stroke); border-radius: 16px; background: var(--panel);
         backdrop-filter: blur(10px);
         overflow: hidden;
       }}
       .card .hd {{
         display:flex; align-items: center; justify-content: space-between; gap: 10px;
         padding: 12px 14px; border-bottom: 1px solid rgba(255,255,255,0.08);
         background: rgba(255,255,255,0.03);
       }}
       .card .hd .h {{ font-weight: 800; letter-spacing: 0.2px; display:flex; align-items: center; gap: 8px; }}
       .pill {{ font-size: 12px; color: var(--muted); border: 1px solid var(--stroke); padding: 3px 8px; border-radius: 999px; background: rgba(255,255,255,0.04); }}
       .card .bd {{ padding: 12px 14px; }}
       .kpis {{ display: grid; gap: 10px; grid-template-columns: repeat(4, 1fr); }}
       .kpi {{
         border: 1px solid rgba(255,255,255,0.10);
         border-radius: 14px;
         background: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.03));
         padding: 12px 12px;
       }}
       .kpi .lbl {{ color: var(--muted); font-size: 12px; }}
       .kpi .val {{ font-size: 22px; font-weight: 850; margin-top: 6px; letter-spacing: -0.3px; }}
       .kpi .sub {{ color: var(--muted); font-size: 12px; margin-top: 5px; }}
       .good {{ color: var(--good); }}
       .bad {{ color: var(--bad); }}
       .warn {{ color: var(--warn); }}
       table {{ width: 100%; border-collapse: collapse; }}
       th, td {{ padding: 10px 10px; border-bottom: 1px solid rgba(255,255,255,0.07); vertical-align: top; }}
       th {{ text-align: left; color: var(--muted); font-size: 12px; font-weight: 700; }}
       td {{ font-size: 13px; }}
       .row2 {{ color: var(--muted); font-size: 12px; margin-top: 4px; }}
       .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }}
       .tag {{
         display:inline-block; padding: 3px 8px; border-radius: 999px; border: 1px solid rgba(255,255,255,0.12);
         background: rgba(255,255,255,0.04); color: var(--muted); font-size: 12px;
       }}
       .btn {{
         cursor: pointer;
         padding: 8px 10px;
         border-radius: 10px;
         border: 1px solid rgba(255,255,255,0.12);
         background: rgba(255,255,255,0.05);
         color: var(--text);
         font-weight: 700;
         font-size: 12px;
       }}
       .btn:hover {{ background: rgba(255,255,255,0.08); }}
       .split {{ display:flex; gap: 10px; flex-wrap: wrap; }}
       .progress {{
         height: 10px; border-radius: 999px; background: rgba(255,255,255,0.07);
         overflow: hidden; border: 1px solid rgba(255,255,255,0.10);
       }}
       .bar {{
         height: 100%; width: 0%;
         background: linear-gradient(90deg, var(--brand), var(--brand2));
       }}
       .small {{ font-size: 12px; color: var(--muted); }}
       .footer {{ margin-top: 14px; color: var(--muted); font-size: 12px; }}
       .banner {{
         margin-top: 12px;
         padding: 10px 12px;
         border-radius: 14px;
         border: 1px solid rgba(255,255,255,0.14);
         background: rgba(255, 77, 77, 0.10);
         display: none;
       }}
       .banner pre {{
         margin: 8px 0 0;
         white-space: pre-wrap;
         word-break: break-word;
       }}
       .col-12 {{ grid-column: span 12; }}
       .col-8 {{ grid-column: span 8; }}
       .col-6 {{ grid-column: span 6; }}
       .col-4 {{ grid-column: span 4; }}
       .col-3 {{ grid-column: span 3; }}
       @media (max-width: 1100px) {{
         .kpis {{ grid-template-columns: repeat(2, 1fr); }}
         .col-8 {{ grid-column: span 12; }}
         .col-4 {{ grid-column: span 12; }}
         .col-6 {{ grid-column: span 12; }}
       }}
     </style>
   </head>
   <body>
     <div class="wrap">
       <div class="topbar">
         <div class="brand">
           <div class="logo"></div>
           <div>
             <div class="title">SuperSpreader • Dashboard</div>
             <div class="subtitle">
               Local: <span class="mono">{host}:{port}</span> • mode=<b>{mode}</b> • trade=<b>{trade_mode}</b>
             </div>
           </div>
         </div>
         <div class="chips">
           <div class="chip">SQLite: <b class="mono">{sqlite_path}</b></div>
           <div class="chip">Status: <b id="statusText">starting…</b></div>
           <button class="btn" id="refreshBtn">Refresh</button>
           {reset_btn}
         </div>
       </div>
 
       <div class="banner" id="errBanner">
         <div style="font-weight:850;">Something is failing (details)</div>
         <div class="row2" id="errBannerMsg">--</div>
         <pre class="mono small" id="errBannerDetail"></pre>
       </div>
 
       <div class="grid">
         <div class="card col-12">
           <div class="hd">
             <div class="h">Command Center <span class="pill" id="clock">--:--:--</span></div>
             <div class="split">
               <span class="tag">XP <b id="xpVal">0</b></span>
               <span class="tag">Streak <b id="streakVal">0</b></span>
               <span class="tag">Rank <b id="rankVal">Rookie</b></span>
             </div>
           </div>
           <div class="bd">
             <div class="kpis">
               <div class="kpi">
                 <div class="lbl">Total PnL</div>
                 <div class="val" id="pnlTotal">--</div>
                 <div class="sub">uPnL <span id="pnlU">--</span> • rPnL <span id="pnlR">--</span> • <span id="pnlMeta">--</span></div>
               </div>
               <div class="kpi">
                 <div class="lbl">Open Positions</div>
                 <div class="val" id="posCount">--</div>
                 <div class="sub">largest risk & exposure spotlight</div>
               </div>
               <div class="kpi">
                 <div class="lbl">Scanner</div>
                 <div class="val" id="scannerCounts">--</div>
                 <div class="sub">eligible • top watching</div>
               </div>
               <div class="kpi">
                 <div class="lbl">Freshness</div>
                 <div class="val" id="freshVal">--</div>
                 <div class="sub">market DB & tape timestamps</div>
               </div>
             </div>
             <div style="margin-top:12px;">
               <div class="small">Next “level up” progress</div>
               <div class="progress"><div class="bar" id="xpBar"></div></div>
             </div>
           </div>
         </div>
 
         <div class="card col-12">
           <div class="hd">
             <div class="h">GitHub publishing</div>
             <div class="pill" id="ghPubMeta">--</div>
           </div>
           <div class="bd">
             <table>
               <thead>
                 <tr>
                   <th>publisher</th>
                   <th>state</th>
                   <th>target</th>
                   <th>last success</th>
                   <th>last error</th>
                 </tr>
               </thead>
               <tbody id="ghPubRows"></tbody>
             </table>
             <div class="row2">This Rust port keeps the UI but does not publish to GitHub by default.</div>
           </div>
         </div>
 
         <div class="card col-8">
           <div class="hd">
             <div class="h">Watchlist (Top Markets)</div>
             <div class="pill" id="watchMeta">--</div>
           </div>
           <div class="bd">
             <table>
               <thead>
                 <tr>
                   <th>#</th>
                   <th>Market</th>
                   <th class="mono">market_id</th>
                   <th>Vol (24h)</th>
                   <th>Liq</th>
                   <th>Ends</th>
                 </tr>
               </thead>
               <tbody id="watchRows"></tbody>
             </table>
           </div>
         </div>
 
         <div class="card col-4">
           <div class="hd">
             <div class="h">Open Positions</div>
             <div class="pill">latest snapshot</div>
           </div>
           <div class="bd">
             <table>
               <thead>
                 <tr>
                   <th class="mono">market_id</th>
                   <th>pos</th>
                   <th>mark</th>
                   <th>PnL</th>
                 </tr>
               </thead>
               <tbody id="posRows"></tbody>
             </table>
           </div>
         </div>
 
         <div class="card col-4">
           <div class="hd">
             <div class="h">Recently Closed</div>
             <div class="pill">flat • realized</div>
           </div>
           <div class="bd">
             <table>
               <thead>
                 <tr>
                   <th class="mono">market_id</th>
                   <th>rPnL</th>
                 </tr>
               </thead>
               <tbody id="flatRows"></tbody>
             </table>
             <div class="row2">Note: a position “closing” means qty returns to 0; it will disappear from Open Positions.</div>
           </div>
         </div>
 
         <div class="card col-12">
           <div class="hd">
             <div class="h">Active Quotes (what the bot is trying to do)</div>
             <div class="pill">mid • fair • spread • targets • inventory</div>
           </div>
           <div class="bd">
             <table>
               <thead>
                 <tr>
                   <th class="mono">market_id</th>
                   <th>Market</th>
                   <th>mid</th>
                   <th>spr</th>
                   <th>fair</th>
                   <th>our bid</th>
                   <th>our ask</th>
                   <th>inv</th>
                 </tr>
               </thead>
               <tbody id="quoteRows"></tbody>
             </table>
             <div class="row2">Canceled orders in “Order activity” are usually old quotes being replaced as fair/mid changes.</div>
           </div>
         </div>
 
         <div class="card col-6">
           <div class="hd">
             <div class="h">Working Orders (open)</div>
             <div class="pill">currently live quotes/orders</div>
           </div>
           <div class="bd">
             <table>
               <thead>
                 <tr>
                   <th>time</th>
                   <th class="mono">market_id</th>
                   <th>side</th>
                   <th>px</th>
                   <th>size</th>
                   <th>strategy</th>
                 </tr>
               </thead>
               <tbody id="openOrderRows"></tbody>
             </table>
           </div>
         </div>
 
         <div class="card col-6">
           <div class="hd">
             <div class="h">Order Activity (includes cancels)</div>
             <div class="pill">cancel/replace churn is normal for market making</div>
           </div>
           <div class="bd">
             <table>
               <thead>
                 <tr>
                   <th>time</th>
                   <th class="mono">market_id</th>
                   <th>side</th>
                   <th>px</th>
                   <th>size</th>
                   <th>strategy</th>
                   <th>status</th>
                 </tr>
               </thead>
               <tbody id="orderRows"></tbody>
             </table>
           </div>
         </div>
 
         <div class="card col-6">
           <div class="hd">
             <div class="h">Recent Fills</div>
             <div class="pill">executions</div>
           </div>
           <div class="bd">
             <table>
               <thead>
                 <tr>
                   <th>time</th>
                   <th class="mono">market_id</th>
                   <th>side</th>
                   <th>px</th>
                   <th>size</th>
                 </tr>
               </thead>
               <tbody id="fillRows"></tbody>
             </table>
           </div>
         </div>
       </div>
 
       <div class="footer">
         Tip: set <span class="mono">DASHBOARD_ENABLED=0</span> to disable UI.
       </div>
     </div>
 
     <script>
       const fmtUsd = (x) => {{
         if (x === null || x === undefined) return "--";
         const n = Number(x);
         if (!Number.isFinite(n)) return "--";
         const sign = n < 0 ? "-" : "";
         const abs = Math.abs(n);
         const d = abs < 1 ? 4 : 2;
         return sign + "$" + abs.toFixed(d);
       }}
       const fmtNum = (x, d=2) => {{
         if (x === null || x === undefined) return "--";
         const n = Number(x);
         if (!Number.isFinite(n)) return "--";
         return n.toFixed(d);
       }}
       const fmtAgo = (ts) => {{
         if (!ts) return "--";
         const now = Date.now()/1000;
         const s = Math.max(0, now - ts);
         if (s < 2) return "just now";
         if (s < 60) return `${{Math.floor(s)}}s ago`;
         const m = s/60;
         if (m < 60) return `${{Math.floor(m)}}m ago`;
         const h = m/60;
         return `${{h.toFixed(1)}}h ago`;
       }}
       const fmtTs = (ts) => {{
         if (!ts) return "--";
         const d = new Date(Number(ts)*1000);
         return d.toLocaleTimeString();
       }}
       const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
 
       function computeGamification(summary) {{
         const pnl = Number(summary?.pnl?.total_pnl ?? 0);
         const watch = Number(summary?.scanner?.top_count ?? 0);
         const pos = Number(summary?.positions_count ?? 0);
         const xp = Math.max(0, Math.floor(50*watch + 200*pos + 10*Math.max(0,pnl)));
         const level = Math.floor(xp / 1000);
         const into = xp % 1000;
         const pct = clamp(into / 1000, 0, 1);
 
         let rank = "Rookie";
         if (level >= 1) rank = "Operator";
         if (level >= 3) rank = "Sniper";
         if (level >= 6) rank = "Market Maker";
         if (level >= 10) rank = "Legend";
 
         const scannerTs = summary?.scanner?.ts ?? null;
         const fresh = scannerTs ? (Date.now()/1000 - scannerTs) : 1e9;
         const streak = fresh < 120 ? Math.min(99, 1 + Math.floor((120 - fresh) / 10)) : 0;
         return {{ xp, level, pct, rank, streak }};
       }}
 
       async function getJson(path) {{
         const r = await fetch(path, {{ cache: "no-store" }});
         if (!r.ok) {{
           let body = "";
           try {{ body = await r.text(); }} catch (e) {{}}
           throw new Error(`${{path}} -> ${{r.status}}${{body ? ("\\n" + body) : ""}}`);
         }}
         return await r.json();
       }}
 
       function setStatus(ok, msg) {{
         const el = document.getElementById("statusText");
         el.textContent = msg;
         el.className = ok ? "good" : "bad";
       }}
 
       function showBanner(msg, detail) {{
         const b = document.getElementById("errBanner");
         document.getElementById("errBannerMsg").textContent = msg || "--";
         document.getElementById("errBannerDetail").textContent = detail || "";
         b.style.display = "block";
       }}
       function hideBanner() {{
         const b = document.getElementById("errBanner");
         b.style.display = "none";
       }}
 
       function escapeHtml(s) {{
         return (s||"").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#039;");
       }}
 
       function renderPublishers(pubs) {{
         const tb = document.getElementById("ghPubRows");
         tb.innerHTML = "";
         const keys = Object.keys(pubs || {{}}).sort();
         if (!keys.length) {{
           const tr = document.createElement("tr");
           tr.innerHTML = `<td colspan="5" class="small">no publisher status available</td>`;
           tb.appendChild(tr);
           document.getElementById("ghPubMeta").textContent = "--";
           return;
         }}
         for (const k of keys) {{
           const p = pubs[k] || {{}};
           const st = (p.state || "--").toString();
           const cls = st === "ok" ? "good" : (st === "error" ? "bad" : (st === "disabled" ? "warn" : ""));
           const target = escapeHtml(JSON.stringify(p.detail || {{}}));
           const ls = p.last_success_ts ? new Date(Number(p.last_success_ts)*1000).toLocaleString() : "--";
           const err = p.last_error ? escapeHtml(p.last_error.toString()) : "";
           const tr = document.createElement("tr");
           tr.innerHTML = `
             <td class="mono">${{escapeHtml(k)}}</td>
             <td class="${{cls}}"><b>${{escapeHtml(st)}}</b></td>
             <td>${{target}}</td>
             <td>${{escapeHtml(ls)}}</td>
             <td class="mono small">${{err}}</td>
           `;
           tb.appendChild(tr);
         }}
         document.getElementById("ghPubMeta").textContent = "ok";
       }}
 
       function renderWatch(rows) {{
         const tb = document.getElementById("watchRows");
         tb.innerHTML = "";
         for (const r of rows) {{
           const endTxt = r.end_ts ? new Date(Number(r.end_ts)*1000).toLocaleString() : "--";
           const q = (r.question || "(no question)").toString();
           const vol = (r.volume_24h_usd ?? null);
           const liq = (r.liquidity_usd ?? null);
           const tr = document.createElement("tr");
           tr.innerHTML = `
             <td><span class="tag">#${{r.rank}}</span></td>
             <td>
               <div style="font-weight:800; line-height:1.2;">${{escapeHtml(q).slice(0, 140)}}</div>
               <div class="row2">event: <span class="mono">${{escapeHtml((r.event_id||"--").toString())}}</span></div>
             </td>
             <td class="mono">${{escapeHtml((r.market_id||"--").toString())}}</td>
             <td>${{vol === null ? "--" : "$" + Number(vol).toFixed(0)}}</td>
             <td>${{liq === null ? "--" : "$" + Number(liq).toFixed(0)}}</td>
             <td>${{escapeHtml(endTxt)}}</td>
           `;
           tb.appendChild(tr);
         }}
       }}
 
       function renderPositions(rows) {{
         const tb = document.getElementById("posRows");
         tb.innerHTML = "";
         for (const r of rows) {{
           const u = Number(r.unrealized_pnl ?? 0);
           const cls = u >= 0 ? "good" : "bad";
           const tr = document.createElement("tr");
           tr.innerHTML = `
             <td class="mono">${{escapeHtml((r.market_id||"--").toString())}}</td>
             <td>${{fmtNum(r.position, 2)}}</td>
             <td>${{fmtNum(r.mark_price, 3)}}</td>
             <td class="${{cls}}">${{fmtUsd(u)}}</td>
           `;
           tb.appendChild(tr);
         }}
       }}
 
       function renderFlat(rows) {{
         const tb = document.getElementById("flatRows");
         tb.innerHTML = "";
         for (const r of rows) {{
           const rp = Number(r.realized_pnl ?? 0);
           const cls = rp >= 0 ? "good" : "bad";
           const tr = document.createElement("tr");
           tr.innerHTML = `
             <td class="mono">${{escapeHtml((r.market_id||"--").toString())}}</td>
             <td class="${{cls}}">${{fmtUsd(rp)}}</td>
           `;
           tb.appendChild(tr);
         }}
       }}
 
       function renderQuotes(rows) {{
         const tb = document.getElementById("quoteRows");
         tb.innerHTML = "";
         for (const r of rows) {{
           const bid = Number(r.tob_best_bid);
           const ask = Number(r.tob_best_ask);
           const spread = (Number.isFinite(bid) && Number.isFinite(ask)) ? (ask - bid) : null;
           const inv = Number(r.inv_qty ?? 0);
           const src = (r.fair_source || "--").toString();
           const q = (r.question || "").toString();
           const tr = document.createElement("tr");
           tr.innerHTML = `
             <td class="mono">${{escapeHtml((r.market_id||"--").toString())}}</td>
             <td>
               <div style="font-weight:800; line-height:1.2;">${{escapeHtml(q).slice(0, 90) || "--"}}</div>
               <div class="row2">src: <span class="mono">${{escapeHtml(src)}}</span> • updated ${{fmtAgo(r.ts)}}</div>
             </td>
             <td>${{fmtNum(r.mid, 3)}}</td>
             <td>${{fmtNum(spread, 3)}}</td>
             <td>${{fmtNum(r.fair, 3)}}</td>
             <td>${{fmtNum(r.target_bid, 3)}}</td>
             <td>${{fmtNum(r.target_ask, 3)}}</td>
             <td>${{fmtNum(inv, 2)}}</td>
           `;
           tb.appendChild(tr);
         }}
       }}
 
       function renderOrders(rows) {{
         const tb = document.getElementById("orderRows");
         tb.innerHTML = "";
         for (const r of rows) {{
           const side = (r.side || "").toLowerCase();
           const sideCls = side === "buy" ? "good" : (side === "sell" ? "warn" : "");
           const strat = (r?.meta?.strategy || "--").toString();
           const tr = document.createElement("tr");
           tr.innerHTML = `
             <td>${{fmtTs(r.created_ts)}}</td>
             <td class="mono">${{escapeHtml((r.market_id||"--").toString())}}</td>
             <td class="${{sideCls}}"><b>${{escapeHtml((r.side||"--").toString())}}</b></td>
             <td>${{fmtNum(r.price, 3)}}</td>
             <td>${{fmtNum(r.size, 2)}}</td>
             <td class="mono small">${{escapeHtml(strat)}}</td>
             <td><span class="tag">${{escapeHtml((r.status||"--").toString())}}</span></td>
           `;
           tb.appendChild(tr);
         }}
       }}
 
       function renderOpenOrders(rows) {{
         const tb = document.getElementById("openOrderRows");
         tb.innerHTML = "";
         for (const r of rows) {{
           const side = (r.side || "").toLowerCase();
           const sideCls = side === "buy" ? "good" : (side === "sell" ? "warn" : "");
           const strat = (r?.meta?.strategy || "--").toString();
           const tr = document.createElement("tr");
           tr.innerHTML = `
             <td>${{fmtTs(r.created_ts)}}</td>
             <td class="mono">${{escapeHtml((r.market_id||"--").toString())}}</td>
             <td class="${{sideCls}}"><b>${{escapeHtml((r.side||"--").toString())}}</b></td>
             <td>${{fmtNum(r.price, 3)}}</td>
             <td>${{fmtNum(r.size, 2)}}</td>
             <td class="mono small">${{escapeHtml(strat)}}</td>
           `;
           tb.appendChild(tr);
         }}
       }}
 
       function renderFills(rows) {{
         const tb = document.getElementById("fillRows");
         tb.innerHTML = "";
         for (const r of rows) {{
           const side = (r.side || "").toLowerCase();
           const sideCls = side === "buy" ? "good" : (side === "sell" ? "warn" : "");
           const tr = document.createElement("tr");
           tr.innerHTML = `
             <td>${{fmtTs(r.ts)}}</td>
             <td class="mono">${{escapeHtml((r.market_id||"--").toString())}}</td>
             <td class="${{sideCls}}"><b>${{escapeHtml((r.side||"--").toString())}}</b></td>
             <td>${{fmtNum(r.price, 3)}}</td>
             <td>${{fmtNum(r.size, 2)}}</td>
           `;
           tb.appendChild(tr);
         }}
       }}
 
       async function refresh() {{
         try {{
           const [summary, watch, pos, flat, quotes, openOrders, orders, fills, pubs] = await Promise.all([
             getJson("/api/summary"),
             getJson("/api/watchlist?limit=30"),
             getJson("/api/positions?limit=20"),
             getJson("/api/positions?limit=20&only_flat=1"),
             getJson("/api/quotes?limit=20"),
             getJson("/api/open_orders?limit=25"),
             getJson("/api/orders?limit=25"),
             getJson("/api/fills?limit=25"),
             getJson("/api/publishers"),
           ]);
 
           setStatus(true, "live");
           hideBanner();
 
           const blocking = summary?.health?.blocking || [];
           if (blocking.length) {{
             const lines = blocking.slice(0, 6).map((x) => `${{x.component}}: ${{x.message}}`).join("\\n");
             const detail = blocking
               .slice(0, 6)
               .map((x) => `- ${{x.component}} @ ${{fmtAgo(x.ts)}}\\n  ${{x.message}}\\n  ${{x.detail || ""}}`)
               .join("\\n");
             showBanner("Blocking error(s) detected — trading may be paused", lines);
             document.getElementById("errBannerDetail").textContent = detail;
           }}
 
           const pnl = summary.pnl || {{}};
           const total = Number(pnl.total_pnl ?? 0);
           document.getElementById("pnlTotal").textContent = fmtUsd(total);
           document.getElementById("pnlTotal").className = "val " + (total >= 0 ? "good" : "bad");
           document.getElementById("pnlU").textContent = fmtUsd(pnl.total_unrealized ?? 0);
           document.getElementById("pnlR").textContent = fmtUsd(pnl.total_realized ?? 0);
           document.getElementById("pnlMeta").textContent = pnl.ts ? ("updated " + fmtAgo(pnl.ts)) : "no pnl snapshot yet";
 
           document.getElementById("posCount").textContent = String(summary.positions_count ?? 0);
 
           const sc = summary.scanner || null;
           document.getElementById("scannerCounts").textContent = sc ? `${{sc.eligible_count}} • ${{sc.top_count}}` : "--";
 
           const fresh = summary.freshness || {{}};
           const a = [];
           if (fresh.markets_updated_ts) a.push("markets " + fmtAgo(fresh.markets_updated_ts));
           if (fresh.tape_latest_ts) a.push("tape " + fmtAgo(fresh.tape_latest_ts));
           document.getElementById("freshVal").textContent = a.length ? a.join(" • ") : "--";
 
           const g = computeGamification(summary);
           document.getElementById("xpVal").textContent = String(g.xp);
           document.getElementById("streakVal").textContent = String(g.streak);
           document.getElementById("rankVal").textContent = g.rank;
           document.getElementById("xpBar").style.width = `${{Math.round(100*g.pct)}}%`;
 
           document.getElementById("watchMeta").textContent = (watch?.length ? `updated ${{fmtAgo(watch[0].watch_ts)}}` : "--");
 
           renderWatch(watch);
           renderPositions(pos);
           renderFlat(flat);
           renderQuotes(quotes);
           renderOpenOrders(openOrders);
           renderOrders(orders);
           renderFills(fills);
           renderPublishers(pubs);
         }} catch (e) {{
           setStatus(false, "disconnected");
           showBanner("Dashboard refresh failed", (e && e.message) ? e.message : String(e));
         }}
       }}
 
       function tickClock() {{
         const d = new Date();
         document.getElementById("clock").textContent = d.toLocaleTimeString();
       }}
 
       document.getElementById("refreshBtn").addEventListener("click", refresh);
       const resetBtn = document.getElementById("resetBtn");
       if (resetBtn) {{
         resetBtn.addEventListener("click", async () => {{
           const ok = confirm("Reset paper state? This deletes orders/fills/position snapshots/PnL from SQLite.");
           if (!ok) return;
           try {{
             const r = await fetch("/api/admin/reset_paper_state", {{ method: "POST" }});
             if (!r.ok) {{
               const t = await r.text();
               throw new Error(`reset failed: ${{r.status}} ${{t}}`);
             }}
             await refresh();
           }} catch (e) {{
             showBanner("Reset failed", (e && e.message) ? e.message : String(e));
           }}
         }});
       }}
       tickClock();
       setInterval(tickClock, 1000);
       refresh();
       setInterval(refresh, 1500);
     </script>
   </body>
 </html>"#,
        host = host,
        port = port,
        mode = mode,
        trade_mode = trade_mode,
        sqlite_path = sqlite_path,
        reset_btn = reset_btn
    )
}

async fn api_summary(State(st): State<DashboardState>) -> impl IntoResponse {
    let ts = now_ts();
    let pnl = st.store.fetch_latest_pnl().unwrap_or(None);
    let scanner = st.store.fetch_latest_scanner_snapshot().unwrap_or(None);
    let positions = st.store.fetch_latest_positions(500).unwrap_or_default();
    let health = st
        .store
        .fetch_runtime_statuses()
        .unwrap_or_else(|_| serde_json::json!({}));

    let blocking = health
        .as_object()
        .map(|m| {
            m.values()
                .filter(|v| v.get("level").and_then(|x| x.as_str()) == Some("error"))
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let positions_count = positions
        .iter()
        .filter(|p| p.get("position").and_then(|x| x.as_f64()).unwrap_or(0.0) != 0.0)
        .count();

    let freshness = serde_json::json!({
        "markets_updated_ts": st.store.fetch_latest_market_update_ts().ok().flatten(),
        "tape_latest_ts": st.store.fetch_latest_tape_ts().ok().flatten(),
    });

    Json(serde_json::json!({
        "ts": ts,
        "mode": st.settings.run_mode,
        "trade_mode": st.settings.trade_mode,
        "pnl": pnl,
        "scanner": scanner,
        "positions_count": positions_count,
        "freshness": freshness,
        "health": {
            "components": health,
            "blocking": blocking,
        }
    }))
}

async fn api_health(State(st): State<DashboardState>) -> impl IntoResponse {
    let ts = now_ts();
    let health = st
        .store
        .fetch_runtime_statuses()
        .unwrap_or_else(|_| serde_json::json!({}));
    Json(serde_json::json!({ "ts": ts, "components": health }))
}

#[derive(Deserialize)]
struct LimitQ {
    limit: Option<usize>,
}

async fn api_watchlist(
    State(st): State<DashboardState>,
    Query(q): Query<LimitQ>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(30);
    match st.store.fetch_watchlist(limit) {
        Ok(rows) => Json(JsonValue::Array(rows)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct PositionsQ {
    limit: Option<usize>,
    only_flat: Option<i32>,
}

async fn api_positions(
    State(st): State<DashboardState>,
    Query(q): Query<PositionsQ>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(25);
    let only_flat = q.only_flat.unwrap_or(0) == 1;
    match st.store.fetch_latest_positions(limit) {
        Ok(rows) => {
            let filtered = if only_flat {
                rows.into_iter()
                    .filter(|r| {
                        r.get("position").and_then(|x| x.as_f64()).unwrap_or(0.0) == 0.0
                            && r.get("realized_pnl")
                                .and_then(|x| x.as_f64())
                                .unwrap_or(0.0)
                                != 0.0
                    })
                    .collect::<Vec<_>>()
            } else {
                rows.into_iter()
                    .filter(|r| r.get("position").and_then(|x| x.as_f64()).unwrap_or(0.0) != 0.0)
                    .collect::<Vec<_>>()
            };
            Json(JsonValue::Array(filtered)).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn api_orders(
    State(st): State<DashboardState>,
    Query(q): Query<LimitQ>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(50);
    match st.store.fetch_recent_orders(limit, None) {
        Ok(rows) => Json(JsonValue::Array(rows)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn api_open_orders(
    State(st): State<DashboardState>,
    Query(q): Query<LimitQ>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(50);
    match st.store.fetch_recent_orders(limit, Some("open")) {
        Ok(rows) => Json(JsonValue::Array(rows)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn api_quotes(
    State(st): State<DashboardState>,
    Query(q): Query<LimitQ>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(25);
    match st.store.fetch_latest_quotes(limit) {
        Ok(rows) => Json(JsonValue::Array(rows)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn api_fills(State(st): State<DashboardState>, Query(q): Query<LimitQ>) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(100);
    match st.store.fetch_recent_fills(limit) {
        Ok(rows) => Json(JsonValue::Array(rows)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn api_publishers() -> impl IntoResponse {
    // The old Python app supported optional GitHub gist/repo publishing.
    // This Rust port intentionally keeps the dashboard UI but does not publish by default.
    Json(serde_json::json!({}))
}

async fn api_reset_paper_state(State(st): State<DashboardState>) -> impl IntoResponse {
    if st.settings.trade_mode != "paper" {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"ok": false, "error": "reset_only_allowed_in_paper_mode"})),
        )
            .into_response();
    }
    if !st.settings.dashboard_enable_reset {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"ok": false, "error": "reset_disabled"})),
        )
            .into_response();
    }
    if let Err(e) = st.store.clear_trading_state() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"ok": false, "error": e.to_string()})),
        )
            .into_response();
    }
    Json(serde_json::json!({"ok": true, "ts": now_ts()})).into_response()
}

fn now_ts() -> f64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    now.as_secs_f64()
}
