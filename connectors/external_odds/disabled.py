from __future__ import annotations

from connectors.external_odds.base import ExternalOdds, ExternalOddsProvider
from trading.types import MarketInfo


class DisabledOddsProvider(ExternalOddsProvider):
    """
    External odds provider that is intentionally disabled.

    Used when DISALLOW_MOCK_DATA=true to ensure no strategy accidentally queries a mock
    or placeholder external model.
    """

    async def get_fair_prob(self, market: MarketInfo) -> ExternalOdds:
        _ = market
        raise RuntimeError("External odds provider is disabled (DISALLOW_MOCK_DATA=true)")

