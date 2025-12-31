## Local run guide (Rust)

### 1) Install Rust (stable)

This repo pins the toolchain via `rust-toolchain.toml`, but you need `rustup` installed.

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
rustup toolchain install stable --profile minimal
```

### 2) Configure environment

```bash
cd /path/to/SuperSpreader
cp .env.example .env
```

Recommended defaults are already in `.env.example`:
- `TRADE_MODE=paper`
- `EXECUTION_MODE=paper`
- `RUN_MODE=paper`
- `DISALLOW_MOCK_DATA=1`

### 3) Run scanner (market selection only)

```bash
RUN_MODE=scanner cargo run --release
```

You should see the watchlist populate in SQLite and in the dashboard.

### 4) Run paper trader (full bot)

```bash
RUN_MODE=paper TRADE_MODE=paper EXECUTION_MODE=paper cargo run --release
```

Dashboard:
- `http://127.0.0.1:8000/`

### 5) Reset paper state (optional)

Option A (recommended): set env and restart:

```bash
PAPER_RESET_ON_START=1 RUN_MODE=paper cargo run --release
```

Option B: enable the dashboard reset button:
- set `DASHBOARD_ENABLE_RESET=1`, then click “Reset paper state”

### Troubleshooting

- **No markets selected**:
  - Lower `MIN_24H_VOLUME_USD` / `MIN_LIQUIDITY_USD`
  - Increase `TOP_N_MARKETS`

- **Feed looks stale / risk rejects `feed_lag`**:
  - Check your system clock / NTP
  - Relax `REJECT_FEED_LAG_MS` (default 100ms is strict)
  - Ensure outbound websocket/https isn’t blocked

- **Too much churn**:
  - Increase `MM_MIN_QUOTE_LIFE_SECS`
  - Increase `MM_REPRICE_THRESHOLD`

- **No fills**:
  - Ensure `EXECUTION_MODE=paper`
  - Lower `PAPER_MIN_REST_SECS` a bit (but keep realistic; default 1s)
  - Reduce thresholds: `MIN_SPREAD_BPS`, `MIN_UPDATES_MIN`

