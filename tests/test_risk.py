from types import SimpleNamespace

from risk.portfolio import Portfolio
from risk.rules import RiskEngine
from trading.types import Fill, TopOfBook


def test_kill_switch_blocks():
    settings = SimpleNamespace(
        kill_switch=True,
        max_feed_lag_secs=10,
        max_spread=1.0,
        max_pos_per_market=100,
        max_event_exposure=1000,
        daily_loss_limit=1000,
    )
    r = RiskEngine(settings)
    tob = TopOfBook(best_bid=0.49, best_bid_size=10, best_ask=0.51, best_ask_size=10)
    pf = Portfolio()
    out = r.pre_trade_check(
        market_id="m1", event_id="e1", side="buy", price=0.5, size=10, tob=tob, portfolio=pf
    )
    assert out.ok is False
    assert out.reason == "kill_switch"


def test_max_position_blocks():
    settings = SimpleNamespace(
        kill_switch=False,
        max_feed_lag_secs=10,
        max_spread=1.0,
        max_pos_per_market=10,
        max_event_exposure=1000,
        daily_loss_limit=1000,
    )
    r = RiskEngine(settings)
    tob = TopOfBook(best_bid=0.49, best_bid_size=10, best_ask=0.51, best_ask_size=10)
    pf = Portfolio()
    # Create existing long position of 10
    pf.apply_fill(
        Fill(fill_id="f1", order_id="o1", market_id="m1", side="buy", price=0.5, size=10, ts=0),
        event_id="e1",
        fee_bps=0.0,
    )
    out = r.pre_trade_check(
        market_id="m1", event_id="e1", side="buy", price=0.5, size=1, tob=tob, portfolio=pf
    )
    assert out.ok is False
    assert out.reason == "max_pos_per_market"
