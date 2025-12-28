from __future__ import annotations

import asyncio
import time
from dataclasses import asdict
from typing import Any, AsyncIterator

import aiohttp

from storage.sqlite import SqliteStore
from trading.feed import BookEvent, FeedEvent, TradeEvent
from trading.types import TopOfBook, TradeTick
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
        # Track last observed top-of-book (by market). Note: Gamma often leaves bestBid/bestAsk
        # unchanged for long stretches. We still need to "refresh" observation time so risk
        # circuit breakers don't trip purely due to lack of price movement.
        self._last: dict[str, tuple[float | None, float | None]] = {}
        # Track last observed trade price so we can emit TradeEvent best-effort.
        self._last_trade_px: dict[str, float | None] = {}

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

                changed = 0
                observed = 0
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
                    last_trade_px = _to_float(m.get("lastTradePrice") or m.get("last_trade_price"))

                    prev = self._last.get(market_id)
                    cur = (best_bid, best_ask)
                    self._last[market_id] = cur

                    tob = TopOfBook(
                        best_bid=best_bid,
                        best_bid_size=None,
                        best_ask=best_ask,
                        best_ask_size=None,
                        ts=now,
                    )
                    # Only persist to tape when the book meaningfully changes; but always emit
                    # a BookEvent so downstream state refreshes tob.ts (prevents false feed_lag).
                    if prev != cur:
                        self._store.insert_tape(tob.ts, market_id, "tob", asdict(tob))
                        changed += 1
                    observed += 1
                    yield BookEvent(kind="tob", market_id=market_id, tob=tob)

                    # Best-effort trade prints: Gamma exposes lastTradePrice but not a full tape.
                    # We emit a TradeEvent only when it changes, with a heuristic side.
                    prev_trade = self._last_trade_px.get(market_id)
                    self._last_trade_px[market_id] = last_trade_px
                    if last_trade_px is not None and last_trade_px != prev_trade:
                        # Heuristic side inference: if it hits the ask => buy; hits the bid => sell; else compare to mid.
                        side = "buy"
                        try:
                            if best_ask is not None and abs(float(last_trade_px) - float(best_ask)) < 1e-9:
                                side = "buy"
                            elif best_bid is not None and abs(float(last_trade_px) - float(best_bid)) < 1e-9:
                                side = "sell"
                            elif best_bid is not None and best_ask is not None:
                                mid = 0.5 * (float(best_bid) + float(best_ask))
                                side = "buy" if float(last_trade_px) >= mid else "sell"
                        except Exception:
                            side = "buy"

                        trade = TradeTick(
                            market_id=market_id,
                            price=float(last_trade_px),
                            size=1.0,
                            side=side,  # type: ignore[assignment]
                            ts=now,
                        )
                        self._store.insert_tape(trade.ts, market_id, "trade", asdict(trade))
                        yield TradeEvent(kind="trade", market_id=market_id, trade=trade)

                self._store.upsert_runtime_status(
                    component="feed.gamma",
                    level="ok",
                    message=f"gamma polling ok (observed {observed}, changed {changed})",
                    detail=f"poll_secs={self._poll} limit={self._limit} want={len(want)}",
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
