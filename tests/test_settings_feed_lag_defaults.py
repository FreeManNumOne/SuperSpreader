from __future__ import annotations

from config.settings import Settings


def test_default_max_feed_lag_is_larger_for_ws(monkeypatch):
    # Ensure env doesn't override.
    monkeypatch.delenv("MAX_FEED_LAG_SECS", raising=False)
    monkeypatch.setenv("POLYMARKET_FEED", "ws")
    monkeypatch.setenv("USE_LIVE_WS_FEED", "0")

    s = Settings.load()
    assert s.polymarket_feed == "ws"
    assert float(s.max_feed_lag_secs) >= 60.0


def test_max_feed_lag_env_override_wins(monkeypatch):
    monkeypatch.setenv("POLYMARKET_FEED", "ws")
    monkeypatch.setenv("MAX_FEED_LAG_SECS", "7")

    s = Settings.load()
    assert s.polymarket_feed == "ws"
    assert float(s.max_feed_lag_secs) == 7.0

