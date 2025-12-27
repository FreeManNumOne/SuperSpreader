from __future__ import annotations

import abc
from dataclasses import dataclass

from trading.types import Fill, Order, Side, TopOfBook, TradeTick


@dataclass(frozen=True)
class OrderRequest:
    market_id: str
    side: Side
    price: float
    size: float
    meta: dict | None = None


class Broker(abc.ABC):
    @abc.abstractmethod
    async def place_limit(self, req: OrderRequest) -> Order:
        raise NotImplementedError

    @abc.abstractmethod
    async def cancel(self, order_id: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def cancel_all_market(self, market_id: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def on_book(self, market_id: str, tob: TopOfBook) -> list[Fill]:
        """
        Called by the engine whenever top-of-book changes.
        Paper broker uses this to simulate fills.
        """
        raise NotImplementedError

    async def on_trade(self, market_id: str, trade: TradeTick) -> list[Fill]:
        """
        Called by the engine on trade prints (if available).
        Default: no-op.
        """
        _ = market_id
        _ = trade
        return []

