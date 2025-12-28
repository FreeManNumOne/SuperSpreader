from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Literal


Side = Literal["buy", "sell"]


@dataclass(frozen=True)
class MarketInfo:
    market_id: str
    question: str
    event_id: str
    active: bool
    end_ts: float | None
    volume_24h_usd: float
    liquidity_usd: float
    # Optional CLOB identifiers used by the websocket market channel.
    # - condition_id: CLOB condition id (hex string)
    # - clob_token_id: token/asset id for the *primary* outcome (usually "Yes")
    condition_id: str | None = None
    clob_token_id: str | None = None


@dataclass(frozen=True)
class TopOfBook:
    best_bid: float | None
    best_bid_size: float | None
    best_ask: float | None
    best_ask_size: float | None
    ts: float = field(default_factory=lambda: time.time())


@dataclass(frozen=True)
class TradeTick:
    market_id: str
    price: float
    size: float
    side: Side
    ts: float


@dataclass
class Order:
    order_id: str
    market_id: str
    side: Side
    price: float
    size: float
    created_ts: float
    status: Literal["open", "filled", "cancelled", "rejected"]
    filled_size: float = 0.0


@dataclass(frozen=True)
class Fill:
    fill_id: str
    order_id: str
    market_id: str
    side: Side
    price: float
    size: float
    ts: float
    meta: dict[str, Any] = field(default_factory=dict)

