from __future__ import annotations

import time
import uuid
from dataclasses import asdict

from execution.base import Broker, OrderRequest
from storage.sqlite import SqliteStore
from trading.types import Fill, Order, TopOfBook, TradeTick
from utils.logging import get_logger


class ShadowBroker(Broker):
    """
    Shadow execution:
    - Records "would place/cancel" to the DB/logs.
    - Never produces fills (portfolio/PnL stays flat).

    This is intended for validating signal frequency + order churn on live data without
    relying on paper fills.
    """

    def __init__(self, store: SqliteStore):
        self._store = store
        self._log = get_logger(__name__)

    async def place_limit(self, req: OrderRequest) -> Order:
        oid = str(uuid.uuid4())
        now = time.time()
        o = Order(
            order_id=oid,
            market_id=req.market_id,
            side=req.side,
            price=float(req.price),
            size=float(req.size),
            created_ts=now,
            status="rejected",
            filled_size=0.0,
        )
        meta = dict(req.meta or {})
        meta["execution_mode"] = "shadow"
        self._store.insert_order({**asdict(o), "meta": meta})
        self._log.info(
            "order.shadow",
            order_id=o.order_id,
            market_id=o.market_id,
            side=o.side,
            price=o.price,
            size=o.size,
            meta=meta,
        )
        return o

    async def cancel(self, order_id: str) -> None:
        # We don't maintain an in-memory blotter; still log intent.
        self._log.info("order.cancel.shadow", order_id=order_id)

    async def cancel_all_market(self, market_id: str) -> None:
        self._log.info("order.cancel_all_market.shadow", market_id=market_id)

    async def on_book(self, market_id: str, tob: TopOfBook) -> list[Fill]:
        _ = market_id
        _ = tob
        return []

    async def on_trade(self, market_id: str, trade: TradeTick) -> list[Fill]:
        _ = market_id
        _ = trade
        return []
