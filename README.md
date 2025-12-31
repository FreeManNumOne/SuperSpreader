## SuperSpreader (Rust) — Polymarket CLOB HFT bot (paper mode)

Production-ready Rust bot for **high-frequency microstructure market making / spread capture** on Polymarket’s CLOB, using **live internal order-book data only** (no external alpha). It uses:
- `polymarket-hft = 0.0.6` for **Gamma market discovery**, **CLOB REST orderbooks**, and **CLOB WebSocket market feed**
- `Tokio` async runtime for low-latency loops (default \(<50ms\))
- **Paper execution** with realistic frictions and a maker-touch fill model
- A built-in **web dashboard** (kept from the prior Python version; now served by Rust) reading the same SQLite schema

### What’s in this repo now

- **Rust app**: `Cargo.toml`, `src/` (bot + dashboard)
- **SQLite telemetry**: `./data/polymarket_trader.sqlite`
- **Markdown snapshot**: `ops/telemetry/latest.md`
- **Legacy Python code**: still present for reference, but Rust is the supported runtime now.

### Prereqs (one-time)

- Install Rust toolchain (this repo pins stable via `rust-toolchain.toml`):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup toolchain install stable
```

### Configure

```bash
cp .env.example .env
```

Key knobs to review in `.env`:
- **paper mode**: `TRADE_MODE=paper`, `EXECUTION_MODE=paper`, `RUN_MODE=paper`
- **frictions**: `SLIPPAGE_BPS=20`, `LATENCY_BPS=10`, `FEES_BPS=0`
- **market selection**: `MIN_24H_VOLUME_USD`, `MIN_LIQUIDITY_USD`, `MIN_SPREAD_BPS`, `MIN_UPDATES_MIN`
- **paper fills**: `PAPER_FILL_MODEL=maker_touch`, `PAPER_MIN_REST_SECS=1.0`

### Run (scanner only)

```bash
RUN_MODE=scanner cargo run --release
```

### Run (paper trader + dashboard)

```bash
RUN_MODE=paper TRADE_MODE=paper EXECUTION_MODE=paper cargo run --release
```

Dashboard:
- `http://127.0.0.1:8000/` (configurable via `DASHBOARD_HOST` / `DASHBOARD_PORT`)

### Notes / safety

- This implementation is **paper trading only**. It consumes live public data, simulates fills locally, and never sends live orders.
- If you want to validate signal rate without fills: set `EXECUTION_MODE=shadow`.

