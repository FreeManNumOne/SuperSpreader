from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from connectors.polymarket.gamma_poll_stream import PolymarketGammaPollStream
from storage.sqlite import SqliteStore


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        return None

    async def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def get(self, url, params=None):  # noqa: ARG002 - signature matches aiohttp usage
        return _FakeResp(self._payload)


@pytest.mark.asyncio
async def test_gamma_feed_emits_heartbeat_events_when_prices_unchanged(monkeypatch, tmp_path):
    """
    Regression: the Gamma poll feed used to emit BookEvents only when bestBid/bestAsk changed.
    Risk uses tob.ts for feed-lag checks, so unchanged prices would look "stale" and reject all orders.
    """
    payload = [{"id": "m1", "bestBid": "0.49", "bestAsk": "0.51"}]

    import connectors.polymarket.gamma_poll_stream as mod

    monkeypatch.setattr(mod.aiohttp, "ClientSession", lambda *a, **k: _FakeSession(payload))  # type: ignore[arg-type]

    store = SqliteStore(str(tmp_path / "t.sqlite"))
    store.init_db()
    s = PolymarketGammaPollStream(store=store, poll_secs=0.25, limit=10)

    gen = s.events(lambda: ["m1"])
    ev1 = await asyncio.wait_for(gen.__anext__(), timeout=2.0)
    ev2 = await asyncio.wait_for(gen.__anext__(), timeout=2.0)

    assert ev1.market_id == "m1"
    assert ev2.market_id == "m1"
    assert ev2.tob.best_bid == ev1.tob.best_bid
    assert ev2.tob.best_ask == ev1.tob.best_ask
    assert ev2.tob.ts > ev1.tob.ts

