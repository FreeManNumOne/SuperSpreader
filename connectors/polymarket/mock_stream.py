from __future__ import annotations

import asyncio
import random
import time
from dataclasses import asdict
from typing import Any, AsyncIterator, Callable

from storage.sqlite import SqliteStore
from trading.feed import BookEvent, FeedEvent, TradeEvent
from trading.types import TopOfBook, TradeTick
from utils.logging import get_logger
from utils.pricing import clamp


class MockPolymarketStream:
    """
    Offline/mock feed that produces synthetic top-of-book + trades for the currently ranked markets.
    Useful for paper trading end-to-end without live connectivity.
    """

    def __init__(self, *, store: SqliteStore, tick_hz: float = 5.0, seed: int = 11):
        self._store = store
        self._dt = 1.0 / max(1e-6, float(tick_hz))
        self._rng = random.Random(seed)
        self._log = get_logger(__name__)
        self._mid: dict[str, float] = {}

    async def events(self, market_ids: Callable[[], list[str]]) -> AsyncIterator[FeedEvent]:
        while True:
            await asyncio.sleep(self._dt)
            mids = list(market_ids() or [])

            if not mids:
                continue

            # Update a subset each tick to approximate async feeds
            for market_id in self._rng.sample(mids, k=min(5, len(mids))):
                mid = self._mid.get(market_id)
                if mid is None:
                    mid = 0.5 + self._rng.uniform(-0.15, 0.15)
                mid = clamp(mid + self._rng.uniform(-0.01, 0.01), 0.02, 0.98)
                self._mid[market_id] = mid

                spread = clamp(abs(self._rng.gauss(0.02, 0.01)), 0.005, 0.12)
                best_bid = clamp(mid - spread / 2.0, 0.01, 0.99)
                best_ask = clamp(mid + spread / 2.0, 0.01, 0.99)
                tob = TopOfBook(
                    best_bid=best_bid,
                    best_bid_size=float(self._rng.uniform(50, 300)),
                    best_ask=best_ask,
                    best_ask_size=float(self._rng.uniform(50, 300)),
                    ts=time.time(),
                )

                self._store.insert_tape(tob.ts, market_id, "tob", asdict(tob))
                yield BookEvent(kind="tob", market_id=market_id, tob=tob)

                # Occasional trades
                if self._rng.random() < 0.3:
                    side = "buy" if self._rng.random() < 0.5 else "sell"
                    px = best_ask if side == "buy" else best_bid
                    trade = TradeTick(
                        market_id=market_id,
                        price=float(px),
                        size=float(self._rng.uniform(5, 50)),
                        side=side,  # type: ignore[assignment]
                        ts=time.time(),
                    )
                    self._store.insert_tape(trade.ts, market_id, "trade", asdict(trade))
                    yield TradeEvent(kind="trade", market_id=market_id, trade=trade)

