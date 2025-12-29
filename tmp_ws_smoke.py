from __future__ import annotations

import asyncio
import os
import time

from connectors.polymarket.market_discovery import PolymarketMarketDiscovery
from connectors.polymarket.ws_stream import PolymarketClobWebSocketStream
from storage.sqlite import SqliteStore


async def main() -> None:
    # Keep this script self-contained and short-lived.
    ws_url = os.getenv("POLYMARKET_WS", "wss://ws-subscriptions-clob.polymarket.com/ws/market").strip()
    seconds = float(os.getenv("WS_SMOKE_SECS", "15"))
    top_n = int(os.getenv("WS_SMOKE_TOP_N", "20"))

    store = SqliteStore(":memory:")
    store.init_db()
    disc = PolymarketMarketDiscovery()
    markets = await disc.fetch_markets(limit=500)

    # Pick markets that actually have CLOB token ids (required for MARKET channel).
    m2 = [m for m in markets if getattr(m, "clob_token_id", None)]
    # Prefer higher-liquidity/volume markets for more frequent updates.
    top, eligible = disc.rank_and_filter(m2, min_vol=0.0, min_liq=0.0, top_n=top_n)
    picks = top or eligible[:top_n]
    subs = [{"market_id": m.market_id, "asset_id": str(m.clob_token_id)} for m in picks if m.clob_token_id]

    print(
        {
            "ts": time.time(),
            "ws_url": ws_url,
            "picked": len(picks),
            "subs": len(subs),
            "sample": subs[:3],
        }
    )

    feed = PolymarketClobWebSocketStream(ws_url, store=store)
    start = time.time()
    n = 0
    kinds: dict[str, int] = {}
    markets_seen: set[str] = set()

    def provider():
        return subs

    async for ev in feed.events(provider):
        n += 1
        kinds[ev.kind] = kinds.get(ev.kind, 0) + 1
        markets_seen.add(ev.market_id)
        if n <= 5:
            print({"ts": time.time(), "event": ev.kind, "market_id": ev.market_id})
        if time.time() - start >= seconds:
            break

    print(
        {
            "ts": time.time(),
            "seconds": round(time.time() - start, 2),
            "events": n,
            "kinds": kinds,
            "markets_seen": len(markets_seen),
        }
    )


if __name__ == "__main__":
    asyncio.run(main())

