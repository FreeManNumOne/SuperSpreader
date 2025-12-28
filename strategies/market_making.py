from __future__ import annotations

import time

from execution.base import OrderRequest
from strategies.base import Strategy, StrategyContext
from utils.logging import get_logger
from utils.pricing import clamp, prob_to_price


class MarketMakingStrategy(Strategy):
    name = "market_making"

    def __init__(self):
        self._log = get_logger(__name__)
        # market_id -> {"bid": order_id, "ask": order_id}
        self._quotes: dict[str, dict[str, str]] = {}
        self._quote_meta: dict[str, dict[str, float]] = {}  # market_id -> {"bid_ts":..., "ask_ts":...}

    async def on_market(self, ctx: StrategyContext, market_id: str) -> None:
        async with ctx.state.lock:
            m = ctx.state.markets.get(market_id)
            tob = ctx.state.tob.get(market_id)
        if m is None or tob is None:
            return
        if tob.best_bid is None or tob.best_ask is None:
            return

        mid = 0.5 * (tob.best_bid + tob.best_ask)
        if bool(getattr(ctx.settings, "disallow_mock_data", False)):
            # Strict mode: do not query any external odds provider at all.
            fair = mid
            fair_source = "book_mid"
            meta = {"strategy": self.name, "fair": fair, "mid": mid, "source": fair_source}
        else:
            # Fair:
            # - In production you might center quotes around an external fair value model.
            # - In this repo's paper/default setup, the ExternalOdds provider is a mock and can be
            #   completely unrelated to the current book, which would place quotes far away and
            #   result in no fills/position changes.
            # So: only trust external fair when it is *not* the mock provider.
            ext = await ctx.odds.get_fair_prob(m)
            ext_fair = prob_to_price(ext.fair_prob)
            ext_source = getattr(ext, "source", None)
            ext_source_norm = str(ext_source or "").lower()
            use_mid = ext_source_norm == "mock"
            fair = mid if use_mid else ext_fair
            fair_source = "book_mid" if use_mid else (ext_source or "external")

            # Meta:
            # - When we fall back to book_mid (mock external odds), do not propagate/log "external_source":"mock"
            #   since the quote was not derived from an external fair.
            meta = {"strategy": self.name, "fair": fair, "mid": mid, "source": fair_source}
            if not use_mid and ext_source is not None:
                meta["external_source"] = ext_source

        # Inventory skew: shift quotes away from current inventory direction
        pos = ctx.portfolio.positions.get(market_id)
        inv = 0.0 if pos is None else float(pos.qty)
        max_pos = max(1.0, float(ctx.settings.max_pos_per_market))
        inv_frac = clamp(inv / max_pos, -1.0, 1.0)
        skew = -inv_frac * float(ctx.settings.mm_inventory_skew) * float(ctx.settings.mm_quote_width)

        width = float(ctx.settings.mm_quote_width)
        target_bid = clamp(fair + skew - width / 2.0, 0.01, 0.99)
        target_ask = clamp(fair + skew + width / 2.0, 0.01, 0.99)
        if target_bid >= target_ask:
            return

        tick = 0.001
        target_bid = round(target_bid / tick) * tick
        target_ask = round(target_ask / tick) * tick

        now = time.time()
        min_life = float(ctx.settings.mm_min_quote_life_secs)
        size = float(ctx.settings.base_order_size)

        q = self._quotes.setdefault(market_id, {})
        qm = self._quote_meta.setdefault(market_id, {})

        # Replace quotes if older than min life or far from target
        await self._ensure_quote(
            ctx=ctx,
            market_id=market_id,
            event_id=m.event_id,
            side="buy",
            target_price=target_bid,
            size=size,
            tob=tob,
            q=q,
            qm=qm,
            now=now,
            min_life=min_life,
            meta=meta,
        )
        await self._ensure_quote(
            ctx=ctx,
            market_id=market_id,
            event_id=m.event_id,
            side="sell",
            target_price=target_ask,
            size=size,
            tob=tob,
            q=q,
            qm=qm,
            now=now,
            min_life=min_life,
            meta=meta,
        )

    async def _ensure_quote(
        self,
        *,
        ctx: StrategyContext,
        market_id: str,
        event_id: str,
        side: str,
        target_price: float,
        size: float,
        tob,
        q: dict[str, str],
        qm: dict[str, float],
        now: float,
        min_life: float,
        meta: dict,
    ) -> None:
        key = "bid" if side == "buy" else "ask"
        ts_key = f"{key}_ts"
        px_key = f"{key}_px"
        oid = q.get(key)
        last_ts = qm.get(ts_key, 0.0)
        last_px = qm.get(px_key, None)

        # Cancel/replace: for this scaffold, we simply cancel and place a new order when needed.
        needs_replace = oid is None or (now - last_ts) >= min_life
        if oid is not None and last_px is not None and abs(float(last_px) - float(target_price)) >= 0.002:
            needs_replace = True
        if not needs_replace:
            return

        # Risk gate
        r = ctx.risk.pre_trade_check(
            market_id=market_id,
            event_id=event_id,
            side=side,  # type: ignore[arg-type]
            price=target_price,
            size=size,
            tob=tob,
            portfolio=ctx.portfolio,
        )
        if not r.ok:
            # If risk blocks, cancel existing quote
            if oid is not None:
                await ctx.broker.cancel(oid)
                q.pop(key, None)
            return

        if oid is not None:
            await ctx.broker.cancel(oid)
            q.pop(key, None)

        o = await ctx.broker.place_limit(
            OrderRequest(market_id=market_id, side=side, price=target_price, size=size, meta=meta)  # type: ignore[arg-type]
        )
        q[key] = o.order_id
        qm[ts_key] = now
        qm[px_key] = float(target_price)
        self._log.info(
            "quote.placed",
            market_id=market_id,
            side=side,
            price=target_price,
            size=size,
            order_id=o.order_id,
        )

