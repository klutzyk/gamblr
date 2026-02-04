# Client for the-odds-api.com API provider
import httpx
import logging
from typing import Any
from app.core.config import settings
from app.services.cache import cached

logger = logging.getLogger(__name__)


class TheOddsClient:
    def __init__(self):
        self.base_url = settings.THEODDS_BASE_URL
        self.api_key = settings.THEODDS_API_KEY
        self._latest_usage: dict[str, int | None] = {
            "requests_last": None,
            "requests_remaining": None,
            "requests_used": None,
        }

    @staticmethod
    def _to_int(value: str | None) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _extract_usage(self, res: httpx.Response) -> dict[str, int | None]:
        usage = {
            "requests_last": self._to_int(res.headers.get("x-requests-last")),
            "requests_remaining": self._to_int(res.headers.get("x-requests-remaining")),
            "requests_used": self._to_int(res.headers.get("x-requests-used")),
        }
        self._latest_usage = usage
        return usage

    def _enforce_budget(self, estimated_cost: int, min_remaining_after_call: int = 3):
        remaining = self._latest_usage.get("requests_remaining")
        if remaining is None:
            return
        if remaining - estimated_cost < min_remaining_after_call:
            raise RuntimeError(
                "Skipping odds request to preserve API quota "
                f"(remaining={remaining}, estimated_cost={estimated_cost})."
            )

    @staticmethod
    def _estimate_cost(
        markets: str,
        regions: str | None = None,
        bookmakers: str | None = None,
    ) -> int:
        # The Odds API quota scales with number of requested markets and
        # region-equivalent selection. If bookmakers is provided, regions are ignored.
        market_count = len([m.strip() for m in markets.split(",") if m.strip()])
        if market_count == 0:
            return 0
        selection_count = 1
        if bookmakers:
            book_count = len([b.strip() for b in bookmakers.split(",") if b.strip()])
            # Up to 10 bookmakers count as one region-equivalent.
            selection_count = max(1, (book_count + 9) // 10)
        elif regions:
            selection_count = max(1, len([r.strip() for r in regions.split(",") if r.strip()]))
        return market_count * selection_count

    def latest_usage(self) -> dict[str, int | None]:
        return dict(self._latest_usage)

    @cached(ttl_seconds=60 * 60)  # 1 hour
    async def get_sports(self):
        logger.info("THE ODDS API CALLED: get_sports")

        url = f"{self.base_url}/sports"
        params = {"api_key": self.api_key}

        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(url, params=params)
            res.raise_for_status()
            return res.json()

    @cached(ttl_seconds=60 * 5)  # 5 minutes
    async def get_odds(
        self,
        sport: str,
        regions: str | None = "us",
        bookmakers: str | None = None,
        markets: str = "h2h",
        odds_format: str = "decimal",
        date_format: str = "iso",
        min_remaining_after_call: int = 3,
    ):
        logger.info("THE ODDS API CALLED: get_odds")

        if bookmakers:
            regions = None

        estimated_cost = self._estimate_cost(
            markets=markets, regions=regions, bookmakers=bookmakers
        )
        self._enforce_budget(
            estimated_cost=estimated_cost,
            min_remaining_after_call=min_remaining_after_call,
        )

        url = f"{self.base_url}/sports/{sport}/odds"
        params: dict[str, Any] = {
            "api_key": self.api_key,
            "markets": markets,
            "oddsFormat": odds_format,
            "dateFormat": date_format,
        }
        if regions:
            params["regions"] = regions
        if bookmakers:
            params["bookmakers"] = bookmakers

        async with httpx.AsyncClient(timeout=20) as client:
            res = await client.get(url, params=params)
            res.raise_for_status()
            usage = self._extract_usage(res)

            return {
                "data": res.json(),
                "usage": usage,
                "estimated_cost": estimated_cost,
            }

    @cached(ttl_seconds=60 * 5)  # 5 minutes (events endpoint is free; keep it fresh)
    async def get_events(self, sport: str):
        logger.info("THE ODDS API CALLED: get_events")

        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{self.base_url}/sports/{sport}/events",
                params={"api_key": self.api_key},
            )
            res.raise_for_status()
            return res.json()

    # event odds (eg. player props)
    @cached(ttl_seconds=60 * 60)
    async def get_event_odds(
        self,
        sport: str,
        event_id: str,
        regions: str | None = "us",
        bookmakers: str | None = None,
        markets: str = "player_points",
        odds_format: str = "decimal",
        date_format: str = "iso",
        min_remaining_after_call: int = 3,
    ):
        logger.info("THE ODDS API CALLED: get_event_odds")

        if bookmakers:
            regions = None

        estimated_cost = self._estimate_cost(
            markets=markets, regions=regions, bookmakers=bookmakers
        )
        self._enforce_budget(
            estimated_cost=estimated_cost,
            min_remaining_after_call=min_remaining_after_call,
        )

        params: dict[str, Any] = {
            "api_key": self.api_key,
            "markets": markets,
            "oddsFormat": odds_format,
            "dateFormat": date_format,
        }
        if regions:
            params["regions"] = regions
        if bookmakers:
            params["bookmakers"] = bookmakers

        async with httpx.AsyncClient(timeout=20) as client:
            res = await client.get(
                f"{self.base_url}/sports/{sport}/events/{event_id}/odds",
                params=params,
            )
            res.raise_for_status()
            usage = self._extract_usage(res)
            return {
                "data": res.json(),
                "usage": usage,
                "estimated_cost": estimated_cost,
            }
