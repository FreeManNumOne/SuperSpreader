from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict
from typing import Any, AsyncIterator

import websockets

from storage.sqlite import SqliteStore
from trading.feed import BookEvent, FeedEvent, TradeEvent
from trading.types import TopOfBook, TradeTick
from utils.logging import get_logger


class PolymarketClobWebSocketStream:
    """
    Polymarket CLOB WebSocket stream.

    Notes:
    - The exact subscribe schema can change; this implementation is defensive and meant as a production-ready starting point.
    - For this scaffold, `MockPolymarketStream` is used by default to guarantee an end-to-end runnable paper system.
    """

    def __init__(self, ws_url: str, store: SqliteStore):
        self.ws_url = ws_url
        self._store = store
        self._log = get_logger(__name__)

    async def events(self, market_ids_provider) -> AsyncIterator[FeedEvent]:
        """
        Connects and yields normalized `FeedEvent`s.
        `market_ids_provider` is a callable returning list[str] of market ids to subscribe to.
        """
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
                    self._log.info("ws.connected", url=self.ws_url)
                    self._store.upsert_runtime_status(
                        component="feed.ws",
                        level="ok",
                        message="websocket connected",
                        detail=f"url={self.ws_url}",
                        ts=time.time(),
                    )
                    backoff = 1.0

                    subscribed: set[str] = set()
                    async def resub() -> None:
                        nonlocal subscribed
                        want = set(market_ids_provider())
                        new = want - subscribed
                        if not new:
                            return
                        # Best-effort subscribe message (update as needed to match current Polymarket ws schema)
                        msg = {"type": "subscribe", "channel": "market", "markets": list(new)}
                        await ws.send(json.dumps(msg))
                        subscribed |= new
                        self._log.info("ws.subscribed", count=len(subscribed))

                    await resub()

                    while True:
                        try:
                            await resub()
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue
                        if not raw:
                            continue
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        ev = _normalize_ws_message(msg)
                        if ev is None:
                            continue

                        # Persist tape for backtests
                        if isinstance(ev, BookEvent):
                            self._store.insert_tape(ev.tob.ts, ev.market_id, "tob", asdict(ev.tob))
                        elif isinstance(ev, TradeEvent):
                            self._store.insert_tape(ev.trade.ts, ev.market_id, "trade", asdict(ev.trade))

                        yield ev
            except Exception:
                self._log.exception("ws.error", url=self.ws_url, backoff=backoff)
                # Keep dashboard concise: record short error + URL.
                self._store.upsert_runtime_status(
                    component="feed.ws",
                    level="error",
                    message="websocket feed error",
                    detail=f"url={self.ws_url} backoff={backoff}",
                    ts=time.time(),
                )
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 2.0)


def _normalize_ws_message(msg: dict[str, Any]) -> FeedEvent | None:
    """
    Best-effort normalization: tries common field patterns for book/trade updates.
    You should adapt this once you inspect Polymarket's current ws payloads.
    """
    if not isinstance(msg, dict):
        return None

    # Common wrapper patterns
    data = msg.get("data") if isinstance(msg.get("data"), dict) else msg

    market_id = data.get("market") or data.get("market_id") or data.get("conditionId") or data.get("id")
    if not market_id:
        return None
    market_id = str(market_id)

    kind = (data.get("type") or data.get("event") or data.get("channel") or "").lower()

    # Orderbook top-of-book style
    for bid_key, ask_key in (("bestBid", "bestAsk"), ("best_bid", "best_ask"), ("bid", "ask")):
        if bid_key in data or ask_key in data:
            try:
                tob = TopOfBook(
                    best_bid=float(data.get(bid_key)) if data.get(bid_key) is not None else None,
                    best_bid_size=float(data.get("bestBidSize") or data.get("bid_size") or data.get("bidSize") or 0.0),
                    best_ask=float(data.get(ask_key)) if data.get(ask_key) is not None else None,
                    best_ask_size=float(data.get("bestAskSize") or data.get("ask_size") or data.get("askSize") or 0.0),
                    ts=float(data.get("ts") or data.get("timestamp") or time.time()),
                )
                return BookEvent(kind="tob", market_id=market_id, tob=tob)
            except Exception:
                return None

    # Trade style
    if "trade" in kind or ("price" in data and "size" in data and "side" in data):
        try:
            trade = TradeTick(
                market_id=market_id,
                price=float(data["price"]),
                size=float(data["size"]),
                side=str(data.get("side", "buy")).lower(),  # type: ignore[assignment]
                ts=float(data.get("ts") or data.get("timestamp") or time.time()),
            )
            if trade.side not in ("buy", "sell"):
                return None
            return TradeEvent(kind="trade", market_id=market_id, trade=trade)
        except Exception:
            return None

    return None

