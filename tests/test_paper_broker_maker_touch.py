import pytest

from execution.paper import PaperBroker
from execution.base import OrderRequest
from storage.sqlite import SqliteStore
from trading.types import TopOfBook


@pytest.mark.asyncio
async def test_maker_touch_fills_bid_when_touch_moves_down(tmp_path):
    store = SqliteStore(str(tmp_path / "t.sqlite"))
    store.init_db()
    broker = PaperBroker(store, fill_model="maker_touch", min_rest_secs=0.0)

    o = await broker.place_limit(OrderRequest(market_id="m1", side="buy", price=0.50, size=10.0, meta={"t": 1}))

    # First TOB seeds prev_tob; no fill.
    fills0 = await broker.on_book("m1", TopOfBook(best_bid=0.50, best_bid_size=1.0, best_ask=0.52, best_ask_size=1.0))
    assert fills0 == []

    # If we were the best bid and the best bid moves down, assume we were hit.
    fills1 = await broker.on_book("m1", TopOfBook(best_bid=0.49, best_bid_size=1.0, best_ask=0.52, best_ask_size=1.0))
    assert len(fills1) == 1
    assert fills1[0].order_id == o.order_id
    assert fills1[0].side == "buy"
    assert fills1[0].price == pytest.approx(0.50)
    assert fills1[0].meta["fill_model"] == "maker_touch"


@pytest.mark.asyncio
async def test_maker_touch_fills_ask_when_touch_moves_up(tmp_path):
    store = SqliteStore(str(tmp_path / "t.sqlite"))
    store.init_db()
    broker = PaperBroker(store, fill_model="maker_touch", min_rest_secs=0.0)

    o = await broker.place_limit(OrderRequest(market_id="m1", side="sell", price=0.51, size=10.0))

    fills0 = await broker.on_book("m1", TopOfBook(best_bid=0.49, best_bid_size=1.0, best_ask=0.51, best_ask_size=1.0))
    assert fills0 == []

    # If we were the best ask and the best ask moves up, assume we were lifted.
    fills1 = await broker.on_book("m1", TopOfBook(best_bid=0.49, best_bid_size=1.0, best_ask=0.52, best_ask_size=1.0))
    assert len(fills1) == 1
    assert fills1[0].order_id == o.order_id
    assert fills1[0].side == "sell"
    assert fills1[0].price == pytest.approx(0.51)
    assert fills1[0].meta["fill_model"] == "maker_touch"

