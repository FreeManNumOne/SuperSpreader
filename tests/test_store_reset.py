from __future__ import annotations

from storage.sqlite import SqliteStore


def test_clear_trading_state_wipes_snapshots(tmp_path):
    store = SqliteStore(str(tmp_path / "t.sqlite"))
    store.init_db()

    store.insert_position_snapshot(
        {
            "ts": 1.0,
            "market_id": "m1",
            "event_id": "e1",
            "position": 10.0,
            "avg_price": 0.5,
            "mark_price": 0.51,
            "unrealized_pnl": 0.1,
            "realized_pnl": 0.0,
        }
    )
    store.insert_pnl_snapshot({"ts": 1.0, "total_unrealized": 0.1, "total_realized": 0.0, "total_pnl": 0.1})
    assert store.fetch_latest_positions(limit=10)
    assert store.fetch_latest_pnl() is not None

    store.clear_trading_state()
    assert store.fetch_latest_positions(limit=10) == []
    assert store.fetch_latest_pnl() is None

