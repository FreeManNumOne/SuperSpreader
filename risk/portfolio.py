from __future__ import annotations

import time
from dataclasses import dataclass, field

from trading.types import Fill, TopOfBook


@dataclass
class Position:
    market_id: str
    event_id: str
    qty: float = 0.0
    avg_price: float = 0.0
    realized_pnl: float = 0.0
    last_mark: float = 0.0
    opened_ts: float = 0.0

    def mark_to_market(self, mark_price: float) -> float:
        self.last_mark = mark_price
        return (mark_price - self.avg_price) * self.qty


@dataclass
class Portfolio:
    positions: dict[str, Position] = field(default_factory=dict)
    day_start_ts: float = field(default_factory=lambda: time.time())

    def get_or_create(self, market_id: str, event_id: str) -> Position:
        p = self.positions.get(market_id)
        if p is None:
            p = Position(market_id=market_id, event_id=event_id)
            self.positions[market_id] = p
        return p

    def apply_fill(self, fill: Fill, event_id: str) -> None:
        """
        Simple PnL model:
        - buy increases qty; sell decreases qty
        - avg_price is maintained for net position
        - realized PnL is booked when reducing an existing position
        """
        p = self.get_or_create(fill.market_id, event_id)
        # Keep event_id fresh in case markets were discovered late.
        p.event_id = event_id
        signed_qty = fill.size if fill.side == "buy" else -fill.size
        old_qty = float(p.qty)
        new_qty = p.qty + signed_qty
        now = float(fill.ts) if getattr(fill, "ts", None) is not None else time.time()

        # If same direction or opening from flat: update weighted avg
        if p.qty == 0 or (p.qty > 0 and signed_qty > 0) or (p.qty < 0 and signed_qty < 0):
            notional = abs(p.qty) * p.avg_price + abs(signed_qty) * fill.price
            p.qty = new_qty
            p.avg_price = (notional / abs(p.qty)) if p.qty != 0 else 0.0
            if old_qty == 0.0 and p.qty != 0.0:
                p.opened_ts = now
            return

        # Reducing / flipping: realize PnL on the closed portion
        closed = min(abs(p.qty), abs(signed_qty))
        # For a long position (p.qty>0), selling at higher price realizes profit
        # For a short position (p.qty<0), buying back lower realizes profit
        if p.qty > 0:
            p.realized_pnl += (fill.price - p.avg_price) * closed
        else:
            p.realized_pnl += (p.avg_price - fill.price) * closed

        p.qty = new_qty
        if p.qty == 0:
            p.avg_price = 0.0
            p.opened_ts = 0.0
        else:
            # flipped: remaining portion takes fill price as new avg
            if (p.qty > 0 and signed_qty > 0) or (p.qty < 0 and signed_qty < 0):
                p.avg_price = fill.price
            # If we flipped sides (crossed through zero), treat as a new position for aging.
            if old_qty != 0.0 and (old_qty > 0 > p.qty or old_qty < 0 < p.qty):
                p.opened_ts = now

    def unrealized_pnl(self, market_id: str, tob: TopOfBook | None) -> float:
        p = self.positions.get(market_id)
        if p is None or tob is None:
            return 0.0
        mark = None
        if tob.best_bid is not None and tob.best_ask is not None:
            mark = 0.5 * (tob.best_bid + tob.best_ask)
        elif tob.best_bid is not None:
            mark = tob.best_bid
        elif tob.best_ask is not None:
            mark = tob.best_ask
        if mark is None:
            return 0.0
        return (mark - p.avg_price) * p.qty

    def total_realized(self) -> float:
        return sum(p.realized_pnl for p in self.positions.values())

