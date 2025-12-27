from __future__ import annotations

import asyncio
import time
from typing import Any

from connectors.external_odds.mock import MockOddsProvider
from connectors.polymarket.market_discovery import PolymarketMarketDiscovery
from connectors.polymarket.mock_stream import MockPolymarketStream
from connectors.polymarket.ws_stream import PolymarketClobWebSocketStream
from execution.base import OrderRequest
from execution.paper import PaperBroker
from risk.portfolio import Portfolio
from risk.rules import RiskEngine
from storage.sqlite import SqliteStore
from strategies.base import StrategyContext
from strategies.cross_venue import CrossVenueFairValueStrategy
from strategies.market_making import MarketMakingStrategy
from trading.feed import BookEvent, FeedEvent, TradeEvent
from trading.state import SharedState
from trading.types import Fill, MarketInfo, TopOfBook, TradeTick
from utils.logging import get_logger


async def run_scanner(settings: Any, store: SqliteStore) -> None:
    state = SharedState()
    discovery = PolymarketMarketDiscovery()
    log = get_logger(__name__)

    while True:
        try:
            markets = await discovery.fetch_markets(limit=500)
            top, eligible = discovery.rank_and_filter(
                markets,
                min_vol=float(settings.min_24h_volume_usd),
                min_liq=float(settings.min_liquidity_usd),
                top_n=int(settings.top_n_markets),
            )
            store.upsert_markets([discovery.to_store_dict(m) for m in eligible])
            ts = time.time()
            store.insert_scanner_snapshot(ts=ts, eligible_count=len(eligible), top_count=len(top))
            store.update_watchlist([m.market_id for m in top], ts=ts)
            async with state.lock:
                state.markets = {m.market_id: m for m in eligible}
                state.ranked_markets = [m.market_id for m in top]
            log.info(
                "scanner.update",
                eligible=len(eligible),
                top=len(top),
                top_ids=[m.market_id for m in top[:5]],
            )
        except Exception:
            log.exception("scanner.error")
        await asyncio.sleep(int(settings.market_refresh_secs))


async def run_paper_trader(settings: Any, store: SqliteStore) -> None:
    if settings.trade_mode != "paper":
        raise RuntimeError("Paper trader requires TRADE_MODE=paper")

    state = SharedState()
    discovery = PolymarketMarketDiscovery()
    odds = MockOddsProvider()
    broker = PaperBroker(store)
    portfolio = Portfolio()
    risk = RiskEngine(settings)
    log = get_logger(__name__)

    strategies = [CrossVenueFairValueStrategy(), MarketMakingStrategy()]
    ctx = StrategyContext(settings=settings, state=state, store=store, broker=broker, risk=risk, portfolio=portfolio, odds=odds)

    live_ws = bool(getattr(settings, "use_live_ws_feed", False))
    if live_ws:
        feed = PolymarketClobWebSocketStream(settings.polymarket_ws, store=store)
    else:
        feed = MockPolymarketStream(store=store, tick_hz=5.0)
    current_market_ids: list[str] = []

    async def scanner_loop() -> None:
        nonlocal current_market_ids
        while True:
            try:
                markets = await discovery.fetch_markets(limit=500)
                top, eligible = discovery.rank_and_filter(
                    markets,
                    min_vol=float(settings.min_24h_volume_usd),
                    min_liq=float(settings.min_liquidity_usd),
                    top_n=int(settings.top_n_markets),
                )
                store.upsert_markets([discovery.to_store_dict(m) for m in eligible])
                ts = time.time()
                store.insert_scanner_snapshot(ts=ts, eligible_count=len(eligible), top_count=len(top))
                store.update_watchlist([m.market_id for m in top], ts=ts)
                async with state.lock:
                    state.markets = {m.market_id: m for m in eligible}
                    state.ranked_markets = [m.market_id for m in top]
                current_market_ids = [m.market_id for m in top]
                log.info("markets.ranked", top=len(top), eligible=len(eligible))
            except Exception:
                log.exception("scanner.error")
            await asyncio.sleep(int(settings.market_refresh_secs))

    async def feed_loop() -> None:
        if live_ws:
            async for ev in feed.events(lambda: list(current_market_ids)):  # type: ignore[arg-type]
                await _handle_feed_event(ctx, ev)
        else:
            async for ev in feed.events(state):  # type: ignore[arg-type]
                await _handle_feed_event(ctx, ev)

    async def strategy_loop() -> None:
        while True:
            await asyncio.sleep(0.25)
            async with state.lock:
                market_ids = list(state.ranked_markets)
            for market_id in market_ids:
                # Time-based stop: close before end if needed
                await _maybe_close_before_end(ctx, market_id)
                for strat in strategies:
                    try:
                        await strat.on_market(ctx, market_id)
                    except Exception:
                        log.exception("strategy.error", strategy=strat.name, market_id=market_id)

    async def snapshot_loop() -> None:
        while True:
            await asyncio.sleep(1.0)
            await _persist_snapshots(ctx)

    await asyncio.gather(scanner_loop(), feed_loop(), strategy_loop(), snapshot_loop())


async def run_backtest(settings: Any, store: SqliteStore) -> None:
    if settings.trade_mode != "paper":
        raise RuntimeError("Backtest requires TRADE_MODE=paper")

    state = SharedState()
    odds = MockOddsProvider(noise=0.0)
    broker = PaperBroker(store)
    portfolio = Portfolio()
    risk = RiskEngine(settings)
    log = get_logger(__name__)

    strategies = [CrossVenueFairValueStrategy(), MarketMakingStrategy()]
    ctx = StrategyContext(settings=settings, state=state, store=store, broker=broker, risk=risk, portfolio=portfolio, odds=odds)

    # Rebuild markets snapshot from the DB is out-of-scope; we trade whatever appears in tape.
    start_ts = float(settings.backtest_start_ts) if settings.backtest_start_ts else None
    end_ts = float(settings.backtest_end_ts) if settings.backtest_end_ts else None
    speed = float(settings.backtest_speed)

    prev_ts = None
    for ts, market_id, kind, payload in store.iter_tape(start_ts, end_ts):
        if prev_ts is not None:
            dt = max(0.0, ts - prev_ts)
            await asyncio.sleep(dt / max(1e-6, speed))
        prev_ts = ts

        # Ensure market exists in state
        async with state.lock:
            if market_id not in state.markets:
                state.markets[market_id] = MarketInfo(
                    market_id=market_id,
                    question=f"tape:{market_id}",
                    event_id=f"event:{market_id}",
                    active=True,
                    end_ts=None,
                    volume_24h_usd=0.0,
                    liquidity_usd=0.0,
                )
            if market_id not in state.ranked_markets:
                state.ranked_markets.append(market_id)

        ev = _payload_to_event(ts, market_id, kind, payload)
        if ev is None:
            continue
        await _handle_feed_event(ctx, ev)

        # Run strategies on every event (simple)
        for strat in strategies:
            try:
                await strat.on_market(ctx, market_id)
            except Exception:
                log.exception("strategy.error", strategy=strat.name, market_id=market_id)

        await _persist_snapshots(ctx)

    log.info("backtest.done")


def _payload_to_event(ts: float, market_id: str, kind: str, payload: dict) -> FeedEvent | None:
    if kind == "tob":
        tob = TopOfBook(
            best_bid=payload.get("best_bid"),
            best_bid_size=payload.get("best_bid_size"),
            best_ask=payload.get("best_ask"),
            best_ask_size=payload.get("best_ask_size"),
            ts=float(payload.get("ts", ts)),
        )
        return BookEvent(kind="tob", market_id=market_id, tob=tob)
    if kind == "trade":
        trade = TradeTick(
            market_id=market_id,
            price=float(payload["price"]),
            size=float(payload["size"]),
            side=payload["side"],
            ts=float(payload.get("ts", ts)),
        )
        return TradeEvent(kind="trade", market_id=market_id, trade=trade)
    return None


async def _handle_feed_event(ctx: StrategyContext, ev: FeedEvent) -> None:
    if isinstance(ev, BookEvent):
        async with ctx.state.lock:
            ctx.state.tob[ev.market_id] = ev.tob
            ctx.state.last_book_update_ts = time.time()
        fills = await ctx.broker.on_book(ev.market_id, ev.tob)
        if fills:
            await _apply_fills(ctx, fills)
    elif isinstance(ev, TradeEvent):
        async with ctx.state.lock:
            ctx.state.last_trade[ev.market_id] = ev.trade
            ctx.state.last_trade_update_ts = time.time()


async def _apply_fills(ctx: StrategyContext, fills: list[Fill]) -> None:
    async with ctx.state.lock:
        for f in fills:
            m = ctx.state.markets.get(f.market_id)
            event_id = m.event_id if m else f"event:{f.market_id}"
            ctx.portfolio.apply_fill(f, event_id=event_id)


async def _persist_snapshots(ctx: StrategyContext) -> None:
    ts = time.time()
    total_u = 0.0
    total_r = float(ctx.portfolio.total_realized())

    async with ctx.state.lock:
        tobs = dict(ctx.state.tob)

    for market_id, pos in ctx.portfolio.positions.items():
        tob = tobs.get(market_id)
        mark = pos.avg_price
        if tob and tob.best_bid is not None and tob.best_ask is not None:
            mark = 0.5 * (tob.best_bid + tob.best_ask)
        elif tob and tob.best_bid is not None:
            mark = tob.best_bid
        elif tob and tob.best_ask is not None:
            mark = tob.best_ask
        pos.last_mark = float(mark)
        u = (float(mark) - float(pos.avg_price)) * float(pos.qty)
        total_u += u

        ctx.store.insert_position_snapshot(
            {
                "ts": ts,
                "market_id": market_id,
                "event_id": pos.event_id,
                "position": float(pos.qty),
                "avg_price": float(pos.avg_price),
                "mark_price": float(mark),
                "unrealized_pnl": float(u),
                "realized_pnl": float(pos.realized_pnl),
            }
        )

    ctx.store.insert_pnl_snapshot({"ts": ts, "total_unrealized": total_u, "total_realized": total_r, "total_pnl": total_u + total_r})


async def _maybe_close_before_end(ctx: StrategyContext, market_id: str) -> None:
    async with ctx.state.lock:
        m = ctx.state.markets.get(market_id)
        tob = ctx.state.tob.get(market_id)
    if m is None or m.end_ts is None or tob is None:
        return
    if tob.best_bid is None or tob.best_ask is None:
        return

    pos = ctx.portfolio.positions.get(market_id)
    if pos is None or pos.qty == 0:
        return

    if (m.end_ts - time.time()) > float(ctx.settings.stop_before_end_secs):
        return

    # Cross the spread to flatten
    if pos.qty > 0:
        side = "sell"
        px = tob.best_bid
        size = abs(float(pos.qty))
    else:
        side = "buy"
        px = tob.best_ask
        size = abs(float(pos.qty))

    r = ctx.risk.pre_trade_check(
        market_id=market_id, event_id=m.event_id, side=side, price=px, size=size, tob=tob, portfolio=ctx.portfolio
    )
    if not r.ok:
        return
    await ctx.broker.place_limit(
        OrderRequest(market_id=market_id, side=side, price=px, size=size, meta={"strategy": "risk_close_before_end"})
    )

