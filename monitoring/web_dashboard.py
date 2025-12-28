from __future__ import annotations

import time
from typing import Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from monitoring.publisher_status import get_publisher_statuses


def build_app(settings: Any, store: Any) -> FastAPI:
    app = FastAPI(title="SuperSpreader Dashboard", version="0.1.0")

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        host = getattr(settings, "dashboard_host", "127.0.0.1")
        port = int(getattr(settings, "dashboard_port", 8000))
        mode = getattr(settings, "run_mode", "?")
        trade_mode = getattr(settings, "trade_mode", "?")
        sqlite_path = getattr(settings, "sqlite_path", "?")
        can_reset = bool(getattr(settings, "trade_mode", "").lower() == "paper") and bool(
            getattr(settings, "dashboard_enable_reset", False)
        )

        # Single-file UI (no build step, no external CDNs).
        return f"""<!doctype html>
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
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji";
        color: var(--text);
        background: radial-gradient(1200px 900px at 15% 10%, rgba(124,92,255,0.20), transparent 60%),
                    radial-gradient(1100px 800px at 90% 20%, rgba(61,214,208,0.16), transparent 55%),
                    radial-gradient(900px 700px at 30% 90%, rgba(255,77,77,0.10), transparent 55%),
                    var(--bg);
      }}
      a {{ color: inherit; }}
      .wrap {{ max-width: 1280px; margin: 0 auto; padding: 22px 18px 42px; }}
      .topbar {{
        display: flex; align-items: center; justify-content: space-between; gap: 12px;
        padding: 16px 16px; border: 1px solid var(--stroke); border-radius: 16px;
        background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03));
        backdrop-filter: blur(10px);
      }}
      .brand {{
        display: flex; align-items: center; gap: 12px;
      }}
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
      .grid {{
        display: grid; gap: 14px; margin-top: 14px;
        grid-template-columns: repeat(12, 1fr);
      }}
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
      .card .hd .h {{
        font-weight: 800; letter-spacing: 0.2px;
        display:flex; align-items: center; gap: 8px;
      }}
      .pill {{
        font-size: 12px; color: var(--muted); border: 1px solid var(--stroke);
        padding: 3px 8px; border-radius: 999px; background: rgba(255,255,255,0.04);
      }}
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
          {"<button class=\"btn\" id=\"resetBtn\" style=\"border-color: rgba(255,77,77,0.45);\">Reset paper state</button>" if can_reset else ""}
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
                <div class="sub">uPnL <span id="pnlU">--</span> • rPnL <span id="pnlR">--</span></div>
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
            <div class="row2">Tip: “github_gist” publishes a private Gist; use the target URL shown here to view it. “github_repo” publishes to a repo file (commits each update).</div>
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

        <div class="card col-6">
          <div class="hd">
            <div class="h">Recent Orders</div>
            <div class="pill">paper/live</div>
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
        Tip: if you don’t want auto-open, set <span class="mono">DASHBOARD_OPEN_BROWSER=0</span>.
      </div>
    </div>

    <script>
      const fmtUsd = (x) => {{
        if (x === null || x === undefined) return "--";
        const n = Number(x);
        if (!Number.isFinite(n)) return "--";
        const sign = n < 0 ? "-" : "";
        const abs = Math.abs(n);
        return sign + "$" + abs.toFixed(2);
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

        // Small “game layer”: XP increases with activity + positive PnL (bounded).
        const xp = Math.max(0, Math.floor(50*watch + 200*pos + 10*Math.max(0,pnl)));
        const level = Math.floor(xp / 1000);
        const into = xp % 1000;
        const pct = clamp(into / 1000, 0, 1);

        let rank = "Rookie";
        if (level >= 1) rank = "Operator";
        if (level >= 3) rank = "Sniper";
        if (level >= 6) rank = "Market Maker";
        if (level >= 10) rank = "Legend";

        // Streak: increment if scanner is fresh; very simple signal.
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

      function renderPublishers(pubs) {{
        const tb = document.getElementById("ghPubRows");
        tb.innerHTML = "";
        const keys = Object.keys(pubs || {{}}).sort();
        if (!keys.length) {{
          const tr = document.createElement("tr");
          tr.innerHTML = `<td colspan="5" class="small">no publisher status available yet</td>`;
          tb.appendChild(tr);
          document.getElementById("ghPubMeta").textContent = "--";
          return;
        }}

        let anyErr = false;
        for (const k of keys) {{
          const p = pubs[k] || {{}};
          const st = (p.state || "--").toString();
          const cls = st === "ok" ? "good" : (st === "error" ? "bad" : (st === "disabled" ? "warn" : ""));
          if (st === "error") anyErr = true;
          const url = p?.detail?.url || null;
          const target = url ? `<a href="${{escapeHtml(url)}}">${{escapeHtml(url)}}</a>` : escapeHtml(JSON.stringify(p.detail || {{}}));
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
        document.getElementById("ghPubMeta").textContent = anyErr ? "errors" : "ok";
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

      function renderOrders(rows) {{
        const tb = document.getElementById("orderRows");
        tb.innerHTML = "";
        for (const r of rows) {{
          const side = (r.side || "").toLowerCase();
          const sideCls = side === "buy" ? "good" : (side === "sell" ? "warn" : "");
          const tr = document.createElement("tr");
          tr.innerHTML = `
            <td>${{fmtTs(r.created_ts)}}</td>
            <td class="mono">${{escapeHtml((r.market_id||"--").toString())}}</td>
            <td class="${{sideCls}}"><b>${{escapeHtml((r.side||"--").toString())}}</b></td>
            <td>${{fmtNum(r.price, 3)}}</td>
            <td>${{fmtNum(r.size, 2)}}</td>
            <td><span class="tag">${{escapeHtml((r.status||"--").toString())}}</span></td>
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

      function escapeHtml(s) {{
        return (s||"").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#039;");
      }}

      async function refresh() {{
        try {{
          const [summary, watch, pos, flat, orders, fills, pubs] = await Promise.all([
            getJson("/api/summary"),
            getJson("/api/watchlist?limit=30"),
            getJson("/api/positions?limit=20"),
            getJson("/api/positions?limit=20&only_flat=1"),
            getJson("/api/orders?limit=25"),
            getJson("/api/fills?limit=25"),
            getJson("/api/publishers"),
          ]);

          setStatus(true, "live");
          hideBanner();

          const pnl = summary.pnl || {{}};
          const total = Number(pnl.total_pnl ?? 0);
          document.getElementById("pnlTotal").textContent = fmtUsd(total);
          document.getElementById("pnlTotal").className = "val " + (total >= 0 ? "good" : "bad");
          document.getElementById("pnlU").textContent = fmtUsd(pnl.total_unrealized ?? 0);
          document.getElementById("pnlR").textContent = fmtUsd(pnl.total_realized ?? 0);

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
</html>
"""

    @app.get("/api/summary", response_class=JSONResponse)
    def summary() -> dict[str, Any]:
        pnl = store.fetch_latest_pnl()
        scanner = store.fetch_latest_scanner_snapshot()
        positions = store.fetch_latest_positions(limit=500)
        return {
            "ts": time.time(),
            "mode": getattr(settings, "run_mode", None),
            "trade_mode": getattr(settings, "trade_mode", None),
            "pnl": pnl,
            "scanner": scanner,
            "positions_count": len([p for p in positions if float(p.get("position") or 0.0) != 0.0]),
            "freshness": {
                "markets_updated_ts": store.fetch_latest_market_update_ts(),
                "tape_latest_ts": store.fetch_latest_tape_ts(),
            },
        }

    @app.get("/api/watchlist", response_class=JSONResponse)
    def watchlist(limit: int = 30) -> list[dict[str, Any]]:
        return store.fetch_watchlist(limit=int(limit))

    @app.get("/api/positions", response_class=JSONResponse)
    def positions(limit: int = 25, only_flat: int = 0) -> list[dict[str, Any]]:
        rows = store.fetch_latest_positions(limit=int(limit))
        if int(only_flat) == 1:
            # Show only flat positions with non-zero realized pnl (recent closes / roundtrips)
            return [
                r
                for r in rows
                if float(r.get("position") or 0.0) == 0.0 and float(r.get("realized_pnl") or 0.0) != 0.0
            ]
        # Default: keep only open positions in the UI view
        return [r for r in rows if float(r.get("position") or 0.0) != 0.0]

    @app.get("/api/orders", response_class=JSONResponse)
    def orders(limit: int = 50) -> list[dict[str, Any]]:
        return store.fetch_recent_orders(limit=int(limit))

    @app.get("/api/fills", response_class=JSONResponse)
    def fills(limit: int = 100) -> list[dict[str, Any]]:
        return store.fetch_recent_fills(limit=int(limit))

    @app.get("/api/publishers", response_class=JSONResponse)
    def publishers() -> dict[str, Any]:
        return get_publisher_statuses()

    @app.post("/api/admin/reset_paper_state", response_class=JSONResponse)
    def reset_paper_state() -> dict[str, Any]:
        if str(getattr(settings, "trade_mode", "")).lower() != "paper":
            return JSONResponse(status_code=400, content={"ok": False, "error": "reset_only_allowed_in_paper_mode"})
        if not bool(getattr(settings, "dashboard_enable_reset", False)):
            return JSONResponse(status_code=403, content={"ok": False, "error": "reset_disabled"})
        try:
            store.clear_trading_state()
        except Exception as e:
            return JSONResponse(status_code=500, content={"ok": False, "error": str(e)[:2000]})
        return {"ok": True, "ts": time.time()}

    return app

