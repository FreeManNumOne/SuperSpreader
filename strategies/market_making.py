from __future__ import annotations

import math
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

        # Tick size / minimum price increment.
        # Default is 0.001 (Polymarket frequently trades in mills).
        tick = float(getattr(ctx.settings, "price_tick", 0.001) or 0.001)
        if not (1e-6 <= tick <= 0.5):
            tick = 0.001

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
        # Ensure a minimum width so bid/ask remain separable on the tick grid.
        width = max(float(ctx.settings.mm_quote_width), 2.0 * tick)
        skew = -inv_frac * float(ctx.settings.mm_inventory_skew) * width

        # Target quotes around fair, but:
        # - do not hard-clamp to 0.01 (many Polymarket markets trade below 1c)
        # - never post crossing quotes (maker-style)
        target_bid = clamp(fair + skew - width / 2.0, tick, 1.0 - tick)
        target_ask = clamp(fair + skew + width / 2.0, tick, 1.0 - tick)

        # Enforce maker quotes vs current TOB (never cross the spread).
        # This prevents accidental taker behavior in paper mode that can create one-sided inventory.
        target_bid = min(target_bid, float(tob.best_ask) - tick)
        target_ask = max(target_ask, float(tob.best_bid) + tick)

        # Round to tick grid conservatively:
        # - bids round DOWN
        # - asks round UP
        target_bid = math.floor(target_bid / tick) * tick
        target_ask = math.ceil(target_ask / tick) * tick

        # Re-apply safety clamps after rounding.
        target_bid = clamp(target_bid, tick, 1.0 - tick)
        target_ask = clamp(target_ask, tick, 1.0 - tick)
        # Re-enforce maker constraints post-rounding (rounding can re-cross on edge cases).
        target_bid = min(target_bid, float(tob.best_ask) - tick)
        target_ask = max(target_ask, float(tob.best_bid) + tick)

        if target_bid >= target_ask:
            return

        now = time.time()
        min_life = float(ctx.settings.mm_min_quote_life_secs)
        size = float(ctx.settings.base_order_size)

        q = self._quotes.setdefault(market_id, {})
        qm = self._quote_meta.setdefault(market_id, {})

        # Persist "what we are trying to do" so the dashboard can explain it.
        # This avoids interpreting raw cancel/replace logs as "random churn".
        try:
            ctx.store.insert_quote_snapshot(
                {
                    "ts": now,
                    "market_id": market_id,
                    "event_id": m.event_id,
                    "tob_best_bid": float(tob.best_bid),
                    "tob_best_ask": float(tob.best_ask),
                    "mid": float(mid),
                    "fair": float(fair),
                    "fair_source": str(fair_source),
                    "inv_qty": float(inv),
                    "width": float(width),
                    "skew": float(skew),
                    "target_bid": float(target_bid),
                    "target_ask": float(target_ask),
                }
            )
        except Exception:
            # Never break trading if telemetry write fails.
            pass

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

