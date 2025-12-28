## Polymarket trading system (Python, asyncio)

Production-oriented scaffold for a Polymarket CLOB trader focused on **high-liquidity / high-volume** markets, supporting:
- **Paper trading end-to-end** (default)
- **Dynamic market discovery + ranking** (24h volume + liquidity thresholds)
- Two strategies:
  - **Cross-venue fair value** vs a pluggable external odds provider (stubbed + mock included)
  - **Market-making / spread capture** with inventory-aware skew and cancel/replace
- **Risk controls**: per-market position, per-event exposure, daily loss limit, kill-switch, feed lag/spread circuit breaker, time-based stop
- **Structured JSON logs**
- **SQLite persistence** for orders/fills + “paper tape” (books/trades) + position/PnL snapshots
- **Backtest runner** that replays recorded tape from SQLite

### Repo layout
- `config/`: env-driven settings
- `connectors/`
  - `polymarket/`: market discovery + WS stream (best-effort) + mock stream
  - `external_odds/`: provider interface + mock provider
- `strategies/`: cross-venue FV + market-making
- `risk/`: portfolio + risk engine
- `execution/`: paper broker + live broker stub (disabled)
- `monitoring/`: CLI dashboard
- `storage/`: SQLite persistence
- `main.py`: entrypoint

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

### Run: market scanner only

```bash
RUN_MODE=scanner TRADE_MODE=paper python main.py
```

When enabled (default), a local dashboard will start at `http://127.0.0.1:8000/` and auto-open in your browser.

### Run: paper trader (scanner + feed + strategies + paper execution)

```bash
RUN_MODE=paper TRADE_MODE=paper python main.py
```

By default, the system runs with a **mock market-data feed** so it is runnable without live connectivity. It records a tape (`tape` table) into `SQLITE_PATH`.

On Windows/macOS/Linux, a good default is:
- `SQLITE_PATH=./data/polymarket_trader.sqlite`

To attempt live WebSocket streaming (best-effort, schema may require adjustment):

```bash
USE_LIVE_WS_FEED=1 RUN_MODE=paper TRADE_MODE=paper python main.py
```

### Run: replay backtest from recorded tape

```bash
RUN_MODE=backtest TRADE_MODE=paper python main.py
```

### Run: using CLI flags (portable)

You can also override the mode via CLI:

```bash
python main.py --mode scanner
python main.py --mode paper
python main.py --mode backtest
```

### Windows PowerShell examples

```powershell
# Scanner
python main.py --mode scanner

# Paper trader (default)
python main.py --mode paper

# Backtest
python main.py --mode backtest

# Optional: change dashboard port + disable auto-open
$env:DASHBOARD_PORT="8010"
$env:DASHBOARD_OPEN_BROWSER="0"
python main.py --mode scanner
```

Backtest controls:
- `BACKTEST_SPEED`: replay speed multiplier (e.g. 50.0)
- `BACKTEST_START_TS` / `BACKTEST_END_TS`: optional unix timestamps

### Dashboard settings

Env vars (in `.env`):
- `DASHBOARD_ENABLED=1`: start the local dashboard server
- `DASHBOARD_HOST=127.0.0.1`
- `DASHBOARD_PORT=8000`
- `DASHBOARD_OPEN_BROWSER=1`: auto-open your browser on start

### Publishing telemetry for remote review (recommended for collaboration)

If you want to run locally but share how the bot is doing, you can publish a periodic snapshot:

- **Option A (cleanest): GitHub Gist** (no repo commits)
  - `GITHUB_PUBLISH_ENABLED=1`
  - `GH_TOKEN=...` (or `GITHUB_TOKEN`)
  - optional `GITHUB_GIST_ID=...` to update an existing gist
  - optional `GITHUB_GIST_ID_FILE=./data/github_gist_id.txt` to keep reusing the same gist across restarts (recommended)

- **Option B: write into a repo file** (creates a commit each update)
  - `GITHUB_REPO_PUBLISH_ENABLED=1`
  - `GITHUB_REPO=owner/name` (e.g. `thepaypay420/SuperSpreader`)
  - `GITHUB_REPO_BRANCH=main`
  - `GITHUB_REPO_PATH=ops/telemetry/latest.md`

Both options share common controls:
- `LOG_FILE=./logs/trader.jsonl` (optional; enables log tail publishing)
- `GITHUB_PUBLISH_INTERVAL_SECS=60`
- `GITHUB_PUBLISH_LOG_TAIL_LINES=200`

### Live-data validation (shadow + stricter paper fills)

Two useful knobs for bridging the gap between paper and production microstructure:

- **Strategy selection (recommended for meaningful paper PnL)**:
  - By default, paper mode runs **microstructure market making** only.
  - Cross-venue FV needs a real external odds model; enable it explicitly:
    - `ENABLE_CROSS_VENUE=1`

- **Shadow execution** (log “would place/cancel”, never fill):
  - `EXECUTION_MODE=shadow`
  - Combine with `USE_LIVE_WS_FEED=1` to validate signal rate and order churn on real markets.

- **More pessimistic paper fills** (only fill on trade prints through your limit):
  - `EXECUTION_MODE=paper`
  - `PAPER_FILL_MODEL=trade_through`
  - Optionally `PAPER_MIN_REST_SECS=1.0` to require orders rest before they can fill.

### Tick size / quoting precision (important for low-price markets)

Polymarket markets frequently trade below 1 cent. To avoid accidentally posting crossing quotes
or hard-clamping prices too high, set:

- `PRICE_TICK=0.001` (default)

### Inventory controls (reduce “dozens of open positions” risk)

Two optional guardrails (disabled by default):

- **Max open positions** (blocks opening new markets once you hit the cap; reduce-only still allowed):
  - `MAX_OPEN_POSITIONS=25` (example)

- **Auto-unwind old positions** (attempts to flatten positions older than a limit):
  - `MAX_POS_AGE_SECS=1800` (example: 30 minutes)
  - `UNWIND_INTERVAL_SECS=10`
  - `UNWIND_MAX_MARKETS_PER_CYCLE=2`

### Paper restart behavior (important)

Paper mode persists telemetry to SQLite. On restart, the system rehydrates the in-memory portfolio from the latest SQLite position snapshots (so risk/unwind and the dashboard stay consistent).

- **Rehydrate portfolio from SQLite on restart** (default on):
  - `PAPER_REHYDRATE_PORTFOLIO=1`
- **Factory reset paper state on startup** (wipes orders/fills/position snapshots/PnL):
  - `PAPER_RESET_ON_START=1`

### Dashboard: optional “Reset paper state” button

To enable a one-click reset in the local dashboard (paper mode only):

- `DASHBOARD_ENABLE_RESET=1`

### Tests

```bash
pytest -q
```

### Safety / live trading

`TRADE_MODE=live` is **disabled by default** and `execution/live.py` currently refuses to run. Implement `LiveBroker` with the official `py-clob-client` once you are satisfied with paper trading and risk controls.

