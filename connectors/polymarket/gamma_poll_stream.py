from __future__ import annotations

import asyncio
import time
from dataclasses import asdict
from typing import Any, AsyncIterator

import aiohttp

from storage.sqlite import SqliteStore
from trading.feed import BookEvent, FeedEvent
from trading.types import TopOfBook
from utils.logging import get_logger


class PolymarketGammaPollStream:
    """
    Live-ish feed via Polymarket's public Gamma API.

    Why:
    - The CLOB websocket endpoint(s) tend to change and frequently break.
    - Gamma already provides bestBid/bestAsk/lastTradePrice for markets.
    - Polling is slower than websockets, but it's stable and keeps the bot trading.

    Emits:
    - BookEvent (top-of-book) when bid/ask changes.
    """

    def __init__(
        self,
        *,
        store: SqliteStore,
        base_url: str = "https://gamma-api.polymarket.com",
        poll_secs: float = 1.0,
        limit: int = 500,
    ):
        self._store = store
        self._base = base_url.rstrip("/")
        self._poll = max(0.25, float(poll_secs))
        self._limit = int(limit)
        self._log = get_logger(__name__)
        self._last: dict[str, tuple[float | None, float | None]] = {}

    async def events(self, market_ids_provider) -> AsyncIterator[FeedEvent]:
        timeout = aiohttp.ClientTimeout(total=20)
        url = f"{self._base}/markets"
        params = {"active": "true", "closed": "false", "limit": str(self._limit), "offset": "0"}

        while True:
            await asyncio.sleep(self._poll)
            want = [str(x) for x in (market_ids_provider() or []) if str(x).strip()]
            if not want:
                continue

            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, params=params) as resp:
                        resp.raise_for_status()
                        data = await resp.json()

                by_id: dict[str, dict[str, Any]] = {}
                if isinstance(data, list):
                    for m in data:
                        if isinstance(m, dict) and (m.get("id") is not None):
                            by_id[str(m.get("id"))] = m

                updated = 0
                now = time.time()
                for market_id in want:
                    m = by_id.get(market_id)
                    if not m:
                        continue

                    def _to_float(v) -> float | None:
                        if v is None:
                            return None
                        try:
                            return float(v)
                        except Exception:
                            return None

                    best_bid = _to_float(m.get("bestBid") or m.get("best_bid"))
                    best_ask = _to_float(m.get("bestAsk") or m.get("best_ask"))

                    prev = self._last.get(market_id)
                    cur = (best_bid, best_ask)
                    if prev == cur:
                        continue
                    self._last[market_id] = cur

                    tob = TopOfBook(
                        best_bid=best_bid,
                        best_bid_size=None,
                        best_ask=best_ask,
                        best_ask_size=None,
                        ts=now,
                    )
                    self._store.insert_tape(tob.ts, market_id, "tob", asdict(tob))
                    updated += 1
                    yield BookEvent(kind="tob", market_id=market_id, tob=tob)

                self._store.upsert_runtime_status(
                    component="feed.gamma",
                    level="ok",
                    message=f"gamma polling ok (updated {updated})",
                    detail=f"poll_secs={self._poll} limit={self._limit}",
                    ts=time.time(),
                )
            except Exception as e:
                self._log.exception("gamma_feed.error")
                self._store.upsert_runtime_status(
                    component="feed.gamma",
                    level="error",
                    message="gamma feed failed",
                    detail=f"{type(e).__name__}: {e}",
                    ts=time.time(),
                )
