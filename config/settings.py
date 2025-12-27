from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv


def _get_env(key: str, default: str | None = None) -> str | None:
    val = os.getenv(key)
    return val if val is not None else default


def _get_int(key: str, default: int) -> int:
    v = _get_env(key)
    return default if v is None or v == "" else int(v)


def _get_float(key: str, default: float) -> float:
    v = _get_env(key)
    return default if v is None or v == "" else float(v)


def _get_bool(key: str, default: bool) -> bool:
    v = _get_env(key)
    if v is None or v == "":
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    # Modes
    trade_mode: str  # paper|live
    run_mode: str  # scanner|paper|backtest

    # Polymarket
    polymarket_host: str
    polymarket_ws: str
    polymarket_chain_id: int
    polymarket_private_key: str | None
    polymarket_api_key: str | None
    polymarket_api_secret: str | None
    polymarket_api_passphrase: str | None
    use_live_ws_feed: bool

    # Market selection
    top_n_markets: int
    min_24h_volume_usd: float
    min_liquidity_usd: float
    market_refresh_secs: int

    # Strategy knobs
    edge_buffer: float
    fees_bps: float
    slippage_bps: float
    latency_bps: float
    base_order_size: float
    min_trade_cooldown_secs: float

    mm_quote_width: float
    mm_inventory_skew: float
    mm_min_quote_life_secs: float
    mm_max_orders_per_market: int

    # Risk
    max_pos_per_market: float
    max_event_exposure: float
    daily_loss_limit: float
    kill_switch: bool
    stop_before_end_secs: float

    # Circuit breaker
    max_feed_lag_secs: float
    max_spread: float

    # Storage / logs
    sqlite_path: str
    log_level: str
    json_logs: bool

    # Monitoring (local web dashboard)
    dashboard_enabled: bool
    dashboard_host: str
    dashboard_port: int

    # Backtest
    backtest_speed: float
    backtest_start_ts: str | None
    backtest_end_ts: str | None

    _overrides: dict[str, str] | None = None

    @staticmethod
    def override_env(pairs: dict[str, str]) -> None:
        for k, v in pairs.items():
            os.environ[k] = v

    @classmethod
    def load(cls) -> "Settings":
        load_dotenv(override=False)
        trade_mode = (_get_env("TRADE_MODE", "paper") or "paper").lower()
        if trade_mode not in {"paper", "live"}:
            raise ValueError("TRADE_MODE must be paper|live")
        run_mode = (_get_env("RUN_MODE", "paper") or "paper").lower()
        # Portable default: keep SQLite under the project working directory.
        # Users can override via SQLITE_PATH in their .env.
        default_sqlite_path = os.path.join(".", "data", "polymarket_trader.sqlite")

        return cls(
            trade_mode=trade_mode,
            run_mode=run_mode,
            polymarket_host=_get_env("POLYMARKET_HOST", "https://clob.polymarket.com") or "",
            polymarket_ws=_get_env("POLYMARKET_WS", "wss://ws-subscriptions-clob.polymarket.com/ws") or "",
            polymarket_chain_id=_get_int("POLYMARKET_CHAIN_ID", 137),
            polymarket_private_key=_get_env("POLYMARKET_PRIVATE_KEY"),
            polymarket_api_key=_get_env("POLYMARKET_API_KEY"),
            polymarket_api_secret=_get_env("POLYMARKET_API_SECRET"),
            polymarket_api_passphrase=_get_env("POLYMARKET_API_PASSPHRASE"),
            use_live_ws_feed=_get_bool("USE_LIVE_WS_FEED", False),
            top_n_markets=_get_int("TOP_N_MARKETS", 20),
            min_24h_volume_usd=_get_float("MIN_24H_VOLUME_USD", 20000.0),
            min_liquidity_usd=_get_float("MIN_LIQUIDITY_USD", 5000.0),
            market_refresh_secs=_get_int("MARKET_REFRESH_SECS", 60),
            edge_buffer=_get_float("EDGE_BUFFER", 0.01),
            fees_bps=_get_float("FEES_BPS", 20.0),
            slippage_bps=_get_float("SLIPPAGE_BPS", 10.0),
            latency_bps=_get_float("LATENCY_BPS", 5.0),
            base_order_size=_get_float("BASE_ORDER_SIZE", 10.0),
            min_trade_cooldown_secs=_get_float("MIN_TRADE_COOLDOWN_SECS", 5.0),
            mm_quote_width=_get_float("MM_QUOTE_WIDTH", 0.02),
            mm_inventory_skew=_get_float("MM_INVENTORY_SKEW", 0.5),
            mm_min_quote_life_secs=_get_float("MM_MIN_QUOTE_LIFE_SECS", 2.0),
            mm_max_orders_per_market=_get_int("MM_MAX_ORDERS_PER_MARKET", 2),
            max_pos_per_market=_get_float("MAX_POS_PER_MARKET", 200.0),
            max_event_exposure=_get_float("MAX_EVENT_EXPOSURE", 500.0),
            daily_loss_limit=_get_float("DAILY_LOSS_LIMIT", 200.0),
            kill_switch=_get_bool("KILL_SWITCH", False),
            stop_before_end_secs=_get_float("STOP_BEFORE_END_SECS", 3600.0),
            max_feed_lag_secs=_get_float("MAX_FEED_LAG_SECS", 5.0),
            max_spread=_get_float("MAX_SPREAD", 0.20),
            sqlite_path=_get_env("SQLITE_PATH", default_sqlite_path) or "",
            log_level=_get_env("LOG_LEVEL", "INFO") or "INFO",
            json_logs=_get_bool("JSON_LOGS", True),
            dashboard_enabled=_get_bool("DASHBOARD_ENABLED", True),
            dashboard_host=_get_env("DASHBOARD_HOST", "127.0.0.1") or "127.0.0.1",
            dashboard_port=_get_int("DASHBOARD_PORT", 8000),
            backtest_speed=_get_float("BACKTEST_SPEED", 50.0),
            backtest_start_ts=_get_env("BACKTEST_START_TS"),
            backtest_end_ts=_get_env("BACKTEST_END_TS"),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            k: getattr(self, k)
            for k in self.__dataclass_fields__.keys()  # type: ignore[attr-defined]
            if not k.startswith("_")
        }

