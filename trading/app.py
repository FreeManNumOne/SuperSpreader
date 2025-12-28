from __future__ import annotations

import asyncio
import time
from typing import Any

from connectors.external_odds.mock import MockOddsProvider
from connectors.polymarket.market_discovery import PolymarketMarketDiscovery
from connectors.polymarket.gamma_poll_stream import PolymarketGammaPollStream
from connectors.polymarket.mock_stream import MockPolymarketStream
from connectors.polymarket.ws_stream import PolymarketClobWebSocketStream
from execution.base import OrderRequest
from execution.paper import PaperBroker
from execution.shadow import ShadowBroker
from risk.portfolio import Portfolio
from risk.portfolio import Position
from risk.rules import RiskEngine
from storage.sqlite import SqliteStore
from strategies.base import StrategyContext
from strategies.cross_venue import CrossVenueFairValueStrategy
from strategies.market_making import MarketMakingStrategy
from trading.feed import BookEvent, FeedEvent, TradeEvent
from trading.state import SharedState
from trading.types import Fill, MarketInfo, TopOfBook, TradeTick
from utils.logging import get_logger

_log = get_logger(__name__)


def _maybe_init_paper_portfolio_from_store(*, settings: Any, store: SqliteStore, portfolio: Portfolio) -> None:
    """
    On restart, the in-memory portfolio starts empty, but the dashboard/risk may show old
    positions from SQLite snapshots. In paper mode we want the bot to:
    - continue managing existing paper positions
    - keep telemetry truthful after restarts

    So we rehydrate positions from the latest per-market snapshot (opt-out via env).
    """
    if str(getattr(settings, "trade_mode", "")).lower() != "paper":
        return

    if bool(getattr(settings, "paper_reset_on_start", False)):
        store.clear_trading_state()
        portfolio.positions.clear()
        _log.warning("paper_state.reset_on_start", sqlite_path=getattr(settings, "sqlite_path", None))
        return

    if not bool(getattr(settings, "paper_rehydrate_portfolio", True)):
        return

    rows = store.fetch_latest_positions(limit=5000)
    if not rows:
        return

    now = time.time()
    loaded_open = 0
    loaded_total = 0
    for r in rows:
        mid = str(r.get("market_id") or "").strip()
        if not mid:
            continue
        qty = float(r.get("position") or 0.0)
        realized = float(r.get("realized_pnl") or 0.0)
        # Keep open positions and (optionally) realized history carriers.
        if qty == 0.0 and realized == 0.0:
            continue
        p = Position(
            market_id=mid,
            event_id=str(r.get("event_id") or f"event:{mid}"),
            qty=float(qty),
            avg_price=float(r.get("avg_price") or 0.0),
            realized_pnl=float(realized),
            last_mark=float(r.get("mark_price") or 0.0),
            opened_ts=(now if qty != 0.0 else 0.0),
        )
        portfolio.positions[mid] = p
        loaded_total += 1
        if qty != 0.0:
            loaded_open += 1

    if loaded_total:
        _log.info(
            "paper_state.rehydrated",
            loaded_total=loaded_total,
            loaded_open=loaded_open,
            sqlite_path=getattr(settings, "sqlite_path", None),
        )


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
    if str(getattr(settings, "execution_mode", "paper")) == "shadow":
        broker = ShadowBroker(store)
    else:
        broker = PaperBroker(
            store,
            fill_model=str(getattr(settings, "paper_fill_model", "on_book_cross")),
            min_rest_secs=float(getattr(settings, "paper_min_rest_secs", 0.0)),
        )
    portfolio = Portfolio()
    _maybe_init_paper_portfolio_from_store(settings=settings, store=store, portfolio=portfolio)
    risk = RiskEngine(settings)
    log = get_logger(__name__)

    # Default to microstructure spread-capture market making in paper mode.
    # Cross-venue FV needs a *real* external odds model; the repo default is a mock.
    enable_cross_venue = bool(getattr(settings, "enable_cross_venue", False))
    strategies = [MarketMakingStrategy()] + ([CrossVenueFairValueStrategy()] if enable_cross_venue else [])
    ctx = StrategyContext(settings=settings, state=state, store=store, broker=broker, risk=risk, portfolio=portfolio, odds=odds)

    feed_mode = str(getattr(settings, "polymarket_feed", "") or "").strip().lower()
    if feed_mode not in {"mock", "gamma", "ws"}:
        feed_mode = "mock"

    if feed_mode == "ws":
        feed = PolymarketClobWebSocketStream(settings.polymarket_ws, store=store)
        store.upsert_runtime_status(
            component="feed",
            level="warn",
            message="using websocket feed (can break if Polymarket changes protocol)",
            detail=f"url={getattr(settings,'polymarket_ws',None)!r}",
            ts=time.time(),
        )
    elif feed_mode == "gamma":
        feed = PolymarketGammaPollStream(store=store, poll_secs=1.0)
        store.upsert_runtime_status(
            component="feed",
            level="ok",
            message="using gamma poll feed",
            detail="source=gamma-api bestBid/bestAsk",
            ts=time.time(),
        )
    else:
        feed = MockPolymarketStream(store=store, tick_hz=5.0)
        store.upsert_runtime_status(
            component="feed",
            level="ok",
            message="using mock feed",
            detail="source=synthetic",
            ts=time.time(),
        )
    current_market_ids: list[str] = []

    def _feed_market_ids() -> list[str]:
        """
        Markets to subscribe the feed to.

        Important: we must keep receiving top-of-book updates for any *held positions*,
        even if they fall out of the scanner's top-N watchlist. Otherwise marks/PnL can
        appear "stuck" and inventory unwind logic may not trigger due to missing books.
        """
        ids: set[str] = set(str(x) for x in (current_market_ids or []) if str(x).strip())
        # Include any currently open positions (qty != 0) so marks stay fresh.
        for mid, pos in (portfolio.positions or {}).items():
            try:
                if float(getattr(pos, "qty", 0.0) or 0.0) != 0.0:
                    ids.add(str(mid))
            except Exception:
                # Be defensive: never break the feed loop due to a bad position row.
                continue
        return list(ids)

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
                store.upsert_runtime_status(
                    component="scanner",
                    level="ok",
                    message=f"scanner ok (eligible={len(eligible)} top={len(top)})",
                    ts=time.time(),
                )
            except Exception:
                log.exception("scanner.error")
                store.upsert_runtime_status(
                    component="scanner",
                    level="error",
                    message="scanner failed",
                    detail="see logs for stacktrace",
                    ts=time.time(),
                )
            await asyncio.sleep(int(settings.market_refresh_secs))

    async def feed_loop() -> None:
        # All feed implementations accept a market id provider, except the mock feed which
        # uses in-memory state (kept for offline mode).
        if feed_mode == "mock":
            async for ev in feed.events(state):  # type: ignore[arg-type]
                await _handle_feed_event(ctx, ev)
        else:
            async for ev in feed.events(_feed_market_ids):  # type: ignore[arg-type]
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

    async def unwind_loop() -> None:
        await _inventory_unwind_loop(ctx)

    await asyncio.gather(scanner_loop(), feed_loop(), strategy_loop(), snapshot_loop(), unwind_loop())


async def run_backtest(settings: Any, store: SqliteStore) -> None:
    if settings.trade_mode != "paper":
        raise RuntimeError("Backtest requires TRADE_MODE=paper")

    state = SharedState()
    odds = MockOddsProvider(noise=0.0)
    if str(getattr(settings, "execution_mode", "paper")) == "shadow":
        broker = ShadowBroker(store)
    else:
        broker = PaperBroker(
            store,
            fill_model=str(getattr(settings, "paper_fill_model", "on_book_cross")),
            min_rest_secs=float(getattr(settings, "paper_min_rest_secs", 0.0)),
        )
    portfolio = Portfolio()
    _maybe_init_paper_portfolio_from_store(settings=settings, store=store, portfolio=portfolio)
    risk = RiskEngine(settings)
    log = get_logger(__name__)

    enable_cross_venue = bool(getattr(settings, "enable_cross_venue", False))
    strategies = [MarketMakingStrategy()] + ([CrossVenueFairValueStrategy()] if enable_cross_venue else [])
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

        # Optional inventory manager (same logic as live paper runner)
        await _inventory_unwind_once(ctx)
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
        fills = await ctx.broker.on_trade(ev.market_id, ev.trade)
        if fills:
            await _apply_fills(ctx, fills)


async def _apply_fills(ctx: StrategyContext, fills: list[Fill]) -> None:
    async with ctx.state.lock:
        for f in fills:
            m = ctx.state.markets.get(f.market_id)
            event_id = m.event_id if m else f"event:{f.market_id}"
            p0 = ctx.portfolio.positions.get(f.market_id)
            qty0 = 0.0 if p0 is None else float(p0.qty)
            avg0 = 0.0 if p0 is None else float(p0.avg_price)
            r0 = 0.0 if p0 is None else float(p0.realized_pnl)

            # Apply basic fees into paper PnL (keeps results from being wildly optimistic).
            ctx.portfolio.apply_fill(f, event_id=event_id, fee_bps=float(getattr(ctx.settings, "fees_bps", 0.0) or 0.0))

            p1 = ctx.portfolio.positions.get(f.market_id)
            qty1 = 0.0 if p1 is None else float(p1.qty)
            avg1 = 0.0 if p1 is None else float(p1.avg_price)
            r1 = 0.0 if p1 is None else float(p1.realized_pnl)

            _log.info(
                "portfolio.fill_applied",
                fill_id=f.fill_id,
                order_id=f.order_id,
                market_id=f.market_id,
                event_id=event_id,
                side=f.side,
                price=f.price,
                size=f.size,
                meta=f.meta,
                pos_before=qty0,
                avg_before=avg0,
                realized_before=r0,
                pos_after=qty1,
                avg_after=avg1,
                realized_after=r1,
                realized_delta=(r1 - r0),
            )

            if qty0 != 0.0 and qty1 == 0.0:
                _log.info(
                    "portfolio.position_flat",
                    market_id=f.market_id,
                    event_id=event_id,
                    realized_pnl=r1,
                    meta=f.meta,
                )


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


async def _inventory_unwind_loop(ctx: StrategyContext) -> None:
    """
    Background loop to keep the bot from accumulating dozens of unattended positions.
    - If a position is older than MAX_POS_AGE_SECS -> attempt to flatten it.
    - If open position count exceeds MAX_OPEN_POSITIONS -> flatten oldest positions until under cap.
    """
    max_open = int(getattr(ctx.settings, "max_open_positions", 0) or 0)
    max_age = float(getattr(ctx.settings, "max_pos_age_secs", 0.0) or 0.0)
    interval = float(getattr(ctx.settings, "unwind_interval_secs", 10.0) or 10.0)
    max_per_cycle = int(getattr(ctx.settings, "unwind_max_markets_per_cycle", 2) or 2)

    if max_open <= 0 and max_age <= 0:
        while True:
            await asyncio.sleep(3600)

    last_unwind_ts: dict[str, float] = {}
    while True:
        try:
            await _inventory_unwind_once(ctx, last_unwind_ts=last_unwind_ts, max_per_cycle=max_per_cycle)
        except Exception:
            _log.exception("risk.inventory_unwind.error")
        await asyncio.sleep(max(1.0, interval))


async def _inventory_unwind_once(
    ctx: StrategyContext,
    *,
    last_unwind_ts: dict[str, float] | None = None,
    max_per_cycle: int | None = None,
) -> None:
    last_unwind_ts = {} if last_unwind_ts is None else last_unwind_ts
    max_per_cycle = 2 if max_per_cycle is None else int(max_per_cycle)

    max_open = int(getattr(ctx.settings, "max_open_positions", 0) or 0)
    max_age = float(getattr(ctx.settings, "max_pos_age_secs", 0.0) or 0.0)

    now = time.time()
    # Snapshot tobs to avoid holding lock across network/IO
    async with ctx.state.lock:
        tobs = dict(ctx.state.tob)

    open_positions = [(mid, p) for mid, p in ctx.portfolio.positions.items() if float(getattr(p, "qty", 0.0) or 0.0) != 0.0]
    open_count = len(open_positions)
    if open_count == 0:
        return

    def age_secs(p) -> float:
        ot = float(getattr(p, "opened_ts", 0.0) or 0.0)
        return 0.0 if ot <= 0 else max(0.0, now - ot)

    # Candidates: too old, or we are over the cap (oldest first).
    candidates: list[tuple[str, Any, str]] = []
    if max_age > 0:
        for mid, p in open_positions:
            if age_secs(p) >= max_age:
                candidates.append((mid, p, "age"))

    if max_open > 0 and open_count > max_open:
        # Add additional candidates (oldest positions) until we could get back under cap.
        need = max(0, open_count - max_open)
        rest = sorted(open_positions, key=lambda r: age_secs(r[1]), reverse=True)
        for mid, p in rest:
            if any(x[0] == mid for x in candidates):
                continue
            candidates.append((mid, p, "cap"))
            need -= 1
            if need <= 0:
                break

    if not candidates:
        return

    # Throttle per market to avoid spamming if fills are slow.
    min_repeat = max(10.0, float(getattr(ctx.settings, "unwind_interval_secs", 10.0) or 10.0))
    did = 0
    for market_id, pos, reason in candidates:
        if did >= max_per_cycle:
            break
        prev = float(last_unwind_ts.get(str(market_id), 0.0))
        if (now - prev) < min_repeat:
            continue

        tob = tobs.get(market_id)
        if tob is None or tob.best_bid is None or tob.best_ask is None:
            continue

        qty = float(getattr(pos, "qty", 0.0) or 0.0)
        if qty == 0.0:
            continue

        # Flatten quickly: sell at best_bid (long), buy at best_ask (short).
        if qty > 0:
            side = "sell"
            px = float(tob.best_bid)
        else:
            side = "buy"
            px = float(tob.best_ask)
        size = abs(qty)

        r = ctx.risk.pre_trade_check(
            market_id=str(market_id),
            event_id=str(getattr(pos, "event_id", f"event:{market_id}")),
            side=side,  # type: ignore[arg-type]
            price=px,
            size=size,
            tob=tob,
            portfolio=ctx.portfolio,
        )
        if not r.ok:
            continue

        # Cancel any existing quotes/orders in this market to stop re-accumulating.
        await ctx.broker.cancel_all_market(str(market_id))
        await ctx.broker.place_limit(
            OrderRequest(
                market_id=str(market_id),
                side=side,  # type: ignore[arg-type]
                price=px,
                size=size,
                meta={
                    "strategy": "risk_inventory_unwind",
                    "reason": reason,
                    "opened_age_secs": age_secs(pos),
                    "open_count": open_count,
                    "max_open_positions": max_open,
                },
            )
        )
        last_unwind_ts[str(market_id)] = now
        did += 1
        _log.info(
            "risk.inventory_unwind.placed",
            market_id=str(market_id),
            side=side,
            price=px,
            size=size,
            reason=reason,
            opened_age_secs=age_secs(pos),
            open_count=open_count,
            max_open_positions=max_open,
        )

