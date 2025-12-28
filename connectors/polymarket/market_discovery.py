from __future__ import annotations

import time
from dataclasses import asdict
from typing import Any

import aiohttp

from trading.types import MarketInfo
from utils.logging import get_logger


class PolymarketMarketDiscovery:
    """
    Market discovery using Polymarket's public Markets/Data API (often referred to as Gamma API).

    This is intentionally defensive about schema: Polymarket APIs evolve, so we try multiple keys.
    """

    def __init__(self, base_url: str = "https://gamma-api.polymarket.com"):
        self.base_url = base_url.rstrip("/")
        self._log = get_logger(__name__)

    async def fetch_markets(self, limit: int = 500) -> list[MarketInfo]:
        url = f"{self.base_url}/markets"
        params = {
            "active": "true",
            "closed": "false",
            "limit": str(limit),
            "offset": "0",
        }
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()

        markets: list[MarketInfo] = []
        if not isinstance(data, list):
            self._log.warning("market_discovery.unexpected_schema", type=str(type(data)))
            return markets

        for m in data:
            if not isinstance(m, dict):
                continue

            market_id = str(m.get("id") or m.get("market_id") or m.get("conditionId") or "")
            if not market_id:
                continue

            question = str(m.get("question") or m.get("title") or "")
            active = bool(m.get("active", True))

            # CLOB identifiers for websocket market channel.
            condition_id = None
            try:
                condition_id = m.get("conditionId") or m.get("condition_id") or m.get("condition")
                condition_id = str(condition_id) if condition_id is not None else None
            except Exception:
                condition_id = None

            clob_token_id = None
            try:
                tok_ids = m.get("clobTokenIds") or m.get("clob_token_ids") or []
                outcomes = m.get("outcomes") or []
                if isinstance(tok_ids, str):
                    # Some APIs return JSON-stringified list.
                    tok_ids = [x.strip() for x in tok_ids.strip("[]").split(",") if x.strip()]
                if isinstance(outcomes, str):
                    outcomes = [x.strip() for x in outcomes.strip("[]").split(",") if x.strip()]
                if isinstance(tok_ids, list) and tok_ids:
                    # Prefer the "Yes" token if we can find it.
                    idx = 0
                    if isinstance(outcomes, list) and outcomes:
                        for i, o in enumerate(outcomes):
                            if str(o or "").strip().lower() == "yes":
                                idx = i
                                break
                    if 0 <= idx < len(tok_ids):
                        clob_token_id = str(tok_ids[idx])
                    else:
                        clob_token_id = str(tok_ids[0])
            except Exception:
                clob_token_id = None

            # event_id is used for exposure grouping
            event_id = str(m.get("event_id") or "")
            if not event_id:
                evs = m.get("events")
                if isinstance(evs, list) and evs:
                    if isinstance(evs[0], dict):
                        event_id = str(evs[0].get("id") or evs[0].get("event_id") or "")
            if not event_id:
                event_id = f"event:{market_id}"

            # end time (optional)
            end_ts = None
            for k in ("endDate", "end_date", "end_time", "resolvedBy", "closeTime"):
                v = m.get(k)
                if isinstance(v, (int, float)):
                    # heuristics: if looks like ms epoch
                    end_ts = float(v / 1000.0) if v > 3_000_000_000 else float(v)
                    break
                if isinstance(v, str) and v:
                    # Avoid parsing ISO timestamps to keep deps minimal; leave None.
                    end_ts = None
                    break

            # volume/liquidity signals (USD)
            volume_24h_usd = 0.0
            for k in ("volume24hr", "volume_24h", "volume24h", "volume", "volumeUsd24h"):
                v = m.get(k)
                if isinstance(v, (int, float, str)) and str(v) != "":
                    try:
                        volume_24h_usd = float(v)
                        break
                    except ValueError:
                        pass

            liquidity_usd = 0.0
            for k in ("liquidity", "liquidity_num", "liquidityUsd", "liquidity_usd"):
                v = m.get(k)
                if isinstance(v, (int, float, str)) and str(v) != "":
                    try:
                        liquidity_usd = float(v)
                        break
                    except ValueError:
                        pass

            markets.append(
                MarketInfo(
                    market_id=market_id,
                    question=question,
                    event_id=event_id,
                    active=active,
                    end_ts=end_ts,
                    volume_24h_usd=volume_24h_usd,
                    liquidity_usd=liquidity_usd,
                    condition_id=condition_id,
                    clob_token_id=clob_token_id,
                )
            )

        self._log.info("market_discovery.fetched", count=len(markets), ts=time.time())
        return markets

    @staticmethod
    def rank_and_filter(
        markets: list[MarketInfo], *, min_vol: float, min_liq: float, top_n: int
    ) -> tuple[list[MarketInfo], list[MarketInfo]]:
        eligible = [m for m in markets if m.active and m.volume_24h_usd >= min_vol and m.liquidity_usd >= min_liq]
        # Score: emphasize volume then liquidity
        eligible.sort(key=lambda m: (m.volume_24h_usd, m.liquidity_usd), reverse=True)
        return eligible[:top_n], eligible

    @staticmethod
    def to_store_dict(m: MarketInfo) -> dict[str, Any]:
        return asdict(m)

