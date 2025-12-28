from types import SimpleNamespace

import pytest

from connectors.external_odds.base import ExternalOdds, ExternalOddsProvider
from execution.base import Broker, OrderRequest
from risk.portfolio import Portfolio
from risk.rules import RiskEngine
from storage.sqlite import SqliteStore
from strategies.base import StrategyContext
from strategies.market_making import MarketMakingStrategy
from trading.state import SharedState
from trading.types import MarketInfo, Order, TopOfBook


class FixedOdds(ExternalOddsProvider):
    def __init__(self, *, fair_prob: float, source: str):
        self._fair_prob = float(fair_prob)
        self._source = str(source)

    async def get_fair_prob(self, market: MarketInfo) -> ExternalOdds:
        _ = market
        return ExternalOdds(fair_prob=self._fair_prob, source=self._source)


class CaptureBroker(Broker):
    def __init__(self):
        self.requests: list[OrderRequest] = []

    async def place_limit(self, req: OrderRequest) -> Order:
        self.requests.append(req)
        return Order(
            order_id=f"oid-{len(self.requests)}",
            market_id=req.market_id,
            side=req.side,
            price=req.price,
            size=req.size,
            created_ts=0.0,
            status="open",
        )

    async def cancel(self, order_id: str) -> None:
        _ = order_id
        return None

    async def cancel_all_market(self, market_id: str) -> None:
        _ = market_id
        return None

    async def on_book(self, market_id: str, tob: TopOfBook):
        _ = market_id
        _ = tob
        return []


@pytest.mark.asyncio
async def test_market_making_ignores_mock_external_fair_and_centers_on_mid(tmp_path):
    """
    Regression: if market making centers around the mock ExternalOdds provider, quotes can be far
    from the book (no fills => positions/PnL appear "stuck"). For mock odds, fair should be mid.
    """
    settings = SimpleNamespace(
        # strategy
        base_order_size=10.0,
        mm_quote_width=0.02,
        mm_inventory_skew=0.0,
        mm_min_quote_life_secs=0.0,
        # risk (set permissive)
        kill_switch=False,
        max_feed_lag_secs=10.0,
        max_spread=1.0,
        max_pos_per_market=1000.0,
        max_event_exposure=1e9,
        daily_loss_limit=1e9,
    )
    store = SqliteStore(str(tmp_path / "t.sqlite"))
    store.init_db()
    state = SharedState()
    market = MarketInfo(
        market_id="m1",
        question="q",
        event_id="e1",
        active=True,
        end_ts=None,
        volume_24h_usd=1e9,
        liquidity_usd=1e9,
    )
    tob = TopOfBook(best_bid=0.014, best_bid_size=10, best_ask=0.017, best_ask_size=10)
    async with state.lock:
        state.markets[market.market_id] = market
        state.tob[market.market_id] = tob

    # Mock external fair far away from mid (similar to user's log snippet)
    odds = FixedOdds(fair_prob=0.70, source="mock")
    broker = CaptureBroker()
    pf = Portfolio()
    risk = RiskEngine(settings)
    ctx = StrategyContext(settings=settings, state=state, store=store, broker=broker, risk=risk, portfolio=pf, odds=odds)

    strat = MarketMakingStrategy()
    await strat.on_market(ctx, "m1")

    assert len(broker.requests) == 2
    buy = [r for r in broker.requests if r.side == "buy"][0]
    sell = [r for r in broker.requests if r.side == "sell"][0]

    # If it used the (mock) external fair (0.70), we'd see prices ~0.69/0.71.
    # With the fix, fair is mid (~0.0155) and the quotes are near the book.
    assert buy.price < 0.1
    assert sell.price < 0.1

    # And since we fell back to book mid, we should not log/propagate "external_source":"mock".
    assert buy.meta["source"] == "book_mid"
    assert sell.meta["source"] == "book_mid"
    assert "external_source" not in buy.meta
    assert "external_source" not in sell.meta

