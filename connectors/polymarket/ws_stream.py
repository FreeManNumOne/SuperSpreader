from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict
from typing import Any, AsyncIterator, Iterable

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
        `market_ids_provider` is a callable returning:
        - list[str] market_ids (legacy / best-effort), OR
        - list[dict] entries like {"market_id": "...", "asset_id": "..."} for MARKET channel subscriptions.

        The Polymarket websocket MARKET channel uses *asset ids* (CLOB token IDs), not Gamma numeric market ids.
        When given the dict form, we maintain an asset_id -> market_id map so downstream code continues to use
        the repo's `market_id` convention.
        """
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(
                    self.ws_url,
                    # We run our own keepalive loop to be explicit/portable across WS servers.
                    ping_interval=None,
                    ping_timeout=None,
                    open_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._log.info("ws.connected", url=self.ws_url)
                    self._store.upsert_runtime_status(
                        component="feed.ws",
                        level="ok",
                        message="websocket connected",
                        detail=f"url={self.ws_url}",
                        ts=time.time(),
                    )
                    backoff = 1.0

                    async def _ping_loop() -> None:
                        # Send websocket *control-frame* pings (not JSON "ping" ops).
                        # Some Polymarket WS endpoints reject JSON ping messages (e.g. "INVALID OPERATION").
                        ping_interval_s = 20.0
                        ping_timeout_s = 20.0
                        while True:
                            await asyncio.sleep(ping_interval_s)
                            if ws.closed:
                                return
                            try:
                                pong_waiter = ws.ping()
                                await asyncio.wait_for(pong_waiter, timeout=ping_timeout_s)
                            except asyncio.CancelledError:
                                raise
                            except Exception:
                                # Force reconnect by closing the socket.
                                try:
                                    await ws.close()
                                except Exception:
                                    pass
                                return

                    keepalive_task = asyncio.create_task(_ping_loop())

                    # Track subscribed asset ids (MARKET channel).
                    subscribed_assets: set[str] = set()
                    asset_to_market_id: dict[str, str] = {}

                    def _want_assets() -> set[str]:
                        want = market_ids_provider() or []
                        out: set[str] = set()
                        asset_to_market_id.clear()
                        # New format: list of {"market_id","asset_id"} entries
                        if want and isinstance(want, list) and isinstance(want[0], dict):
                            for row in want:
                                try:
                                    mid = str(row.get("market_id") or "").strip()
                                    aid = str(row.get("asset_id") or "").strip()
                                except Exception:
                                    continue
                                if not mid or not aid:
                                    continue
                                out.add(aid)
                                asset_to_market_id[aid] = mid
                            return out
                        # Legacy format: list[str]. We can't subscribe properly without asset ids, so return empty.
                        return set()

                    async def resub() -> None:
                        nonlocal subscribed_assets
                        want_assets = _want_assets()
                        new_assets = want_assets - subscribed_assets
                        if not new_assets:
                            return
                        # Polymarket RTDS-style subscription on:
                        #   wss://ws-subscriptions-clob.polymarket.com/ws/market
                        # (the public market/book topic used by the web app)
                        #
                        # The accepted shape is:
                        #   {"action":"subscribe","type":"MARKET","assets_ids":[...]}
                        # where `assets_ids` are CLOB token IDs.
                        msg = {"action": "subscribe", "type": "MARKET", "assets_ids": list(new_assets)}
                        await ws.send(json.dumps(msg))
                        subscribed_assets |= new_assets
                        self._log.info("ws.subscribed", assets=len(subscribed_assets))

                    await resub()

                    try:
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

                            for ev in _normalize_ws_payload(msg, asset_to_market_id=asset_to_market_id):
                                # Persist tape for backtests
                                if isinstance(ev, BookEvent):
                                    self._store.insert_tape(ev.tob.ts, ev.market_id, "tob", asdict(ev.tob))
                                elif isinstance(ev, TradeEvent):
                                    self._store.insert_tape(ev.trade.ts, ev.market_id, "trade", asdict(ev.trade))
                                yield ev
                    finally:
                        keepalive_task.cancel()
                        try:
                            await keepalive_task
                        except Exception:
                            pass
            except Exception as e:
                self._log.exception("ws.error", url=self.ws_url, backoff=backoff)
                # Keep dashboard concise: record short error + URL.
                self._store.upsert_runtime_status(
                    component="feed.ws",
                    level="error",
                    message="websocket feed error",
                    detail=f"url={self.ws_url} backoff={backoff} err={type(e).__name__}:{str(e)[:180]}",
                    ts=time.time(),
                )
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 2.0)


def _normalize_ws_payload(msg: Any, *, asset_to_market_id: dict[str, str] | None = None) -> Iterable[FeedEvent]:
    """
    Polymarket WS endpoints may emit either:
    - a single JSON object (dict), or
    - a JSON array of objects (list[dict]) (e.g. book snapshots).
    """
    asset_to_market_id = {} if asset_to_market_id is None else asset_to_market_id
    if isinstance(msg, dict):
        ev = _normalize_ws_message(msg, asset_to_market_id=asset_to_market_id)
        return [] if ev is None else [ev]
    if isinstance(msg, list):
        out: list[FeedEvent] = []
        for row in msg:
            if not isinstance(row, dict):
                continue
            ev = _normalize_ws_message(row, asset_to_market_id=asset_to_market_id)
            if ev is not None:
                out.append(ev)
        return out
    return []


def _normalize_ws_message(msg: dict[str, Any], *, asset_to_market_id: dict[str, str] | None = None) -> FeedEvent | None:
    """
    Best-effort normalization: tries common field patterns for book/trade updates.
    You should adapt this once you inspect Polymarket's current ws payloads.
    """
    if not isinstance(msg, dict):
        return None
    asset_to_market_id = {} if asset_to_market_id is None else asset_to_market_id

    # `TopOfBook.ts` is used as an observation timestamp (risk feed-lag circuit breaker).
    # Many WS payloads include an exchange/server timestamp that can remain unchanged when
    # bestBid/bestAsk don't move. Using it would create false "feed_lag" rejects in quiet
    # markets even when we're still receiving messages.
    observed_ts = time.time()

    # Common wrapper patterns
    data = msg.get("data") if isinstance(msg.get("data"), dict) else msg

    # Legacy / generic payloads: market_id + bestBid/bestAsk style (used by tests + some WS wrappers).
    market_id_direct = data.get("market") or data.get("market_id") or data.get("conditionId") or data.get("id")
    if market_id_direct:
        market_id_direct = str(market_id_direct)

    event_type = str(data.get("event_type") or data.get("type") or data.get("event") or data.get("channel") or "").lower()

    for bid_key, ask_key in (("bestBid", "bestAsk"), ("best_bid", "best_ask"), ("bid", "ask")):
        if (bid_key in data) or (ask_key in data):
            try:
                tob = TopOfBook(
                    best_bid=float(data.get(bid_key)) if data.get(bid_key) is not None else None,
                    best_bid_size=float(data.get("bestBidSize") or data.get("bid_size") or data.get("bidSize") or 0.0),
                    best_ask=float(data.get(ask_key)) if data.get(ask_key) is not None else None,
                    best_ask_size=float(data.get("bestAskSize") or data.get("ask_size") or data.get("askSize") or 0.0),
                    # Observation time (local) to avoid false feed-lag rejects.
                    ts=observed_ts,
                )
                if market_id_direct:
                    return BookEvent(kind="tob", market_id=market_id_direct, tob=tob)
            except Exception:
                return None

    # Polymarket MARKET channel uses `asset_id` (token id) and `event_type`.
    asset_id = data.get("asset_id") or data.get("assetId") or data.get("asset") or None
    if asset_id is not None:
        asset_id = str(asset_id)
    # Convert asset_id -> our internal market_id (Gamma numeric id) if we have a mapping.
    market_id = asset_to_market_id.get(str(asset_id)) if asset_id else None

    # Market-channel book update: contains bids/asks arrays.
    if event_type == "book" and asset_id and market_id:
        try:
            bids = data.get("bids") or data.get("buys") or []
            asks = data.get("asks") or data.get("sells") or []
            best_bid = None
            best_bid_sz = None
            best_ask = None
            best_ask_sz = None

            def _lvl_to_px_sz(lvl) -> tuple[float | None, float | None]:
                if not isinstance(lvl, dict):
                    return None, None
                px = lvl.get("price")
                sz = lvl.get("size")
                try:
                    px_f = float(px) if px is not None else None
                except Exception:
                    px_f = None
                try:
                    sz_f = float(sz) if sz is not None else None
                except Exception:
                    sz_f = None
                return px_f, sz_f

            if isinstance(bids, list) and bids:
                for lvl in bids:
                    px, sz = _lvl_to_px_sz(lvl)
                    if px is None:
                        continue
                    if best_bid is None or px > best_bid:
                        best_bid = px
                        best_bid_sz = sz
            if isinstance(asks, list) and asks:
                for lvl in asks:
                    px, sz = _lvl_to_px_sz(lvl)
                    if px is None:
                        continue
                    if best_ask is None or px < best_ask:
                        best_ask = px
                        best_ask_sz = sz

            tob = TopOfBook(
                best_bid=best_bid,
                best_bid_size=best_bid_sz,
                best_ask=best_ask,
                best_ask_size=best_ask_sz,
                ts=observed_ts,
            )
            return BookEvent(kind="tob", market_id=str(market_id), tob=tob)
        except Exception:
            return None

    # Trade style
    # Trade messages: either direct market_id or mapped from asset_id.
    trade_market_id = str(market_id_direct) if market_id_direct else (str(market_id) if market_id else None)
    if trade_market_id and ("trade" in event_type or ("price" in data and "size" in data and "side" in data)):
        try:
            raw_ts = data.get("ts") or data.get("timestamp")
            trade_ts = None
            if raw_ts is not None:
                try:
                    trade_ts = float(raw_ts)
                except Exception:
                    trade_ts = None
            # Heuristic: if looks like ms epoch, convert.
            if trade_ts is not None and trade_ts > 3_000_000_000:
                trade_ts = trade_ts / 1000.0
            trade = TradeTick(
                market_id=str(trade_market_id),
                price=float(data["price"]),
                size=float(data["size"]),
                side=str(data.get("side", "buy")).lower(),  # type: ignore[assignment]
                ts=float(trade_ts if trade_ts is not None else observed_ts),
            )
            if trade.side not in ("buy", "sell"):
                return None
            return TradeEvent(kind="trade", market_id=market_id, trade=trade)
        except Exception:
            return None

    return None

