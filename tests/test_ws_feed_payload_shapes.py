from __future__ import annotations

from connectors.polymarket import ws_stream


def test_ws_list_payload_is_supported_for_book_snapshots():
    # The ws-subscriptions-clob /ws/market endpoint commonly sends a JSON list of book objects.
    # Ensure we can normalize those into events.
    asset_id = "token123"
    payload = [
        {
            "event_type": "book",
            "asset_id": asset_id,
            "bids": [{"price": "0.49", "size": "10"}],
            "asks": [{"price": "0.51", "size": "12"}],
        }
    ]

    evs = list(ws_stream._normalize_ws_payload(payload, asset_to_market_id={asset_id: "m1"}))  # noqa: SLF001
    assert len(evs) == 1
    ev = evs[0]
    assert ev.kind == "tob"
    assert ev.market_id == "m1"
    assert ev.tob.best_bid == 0.49
    assert ev.tob.best_ask == 0.51

