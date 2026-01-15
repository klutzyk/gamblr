# Client for the-odds-api.com API provider
import httpx
import logging
from app.core.config import settings
from app.services.cache import cached

logger = logging.getLogger(__name__)


class TheOddsClient:
    def __init__(self):
        self.base_url = settings.THEODDS_BASE_URL
        self.api_key = settings.THEODDS_API_KEY

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
        regions: str = "us",
        markets: str = "h2h",
        odds_format: str = "decimal",
        date_format: str = "iso",
    ):
        logger.info("THE ODDS API CALLED: get_odds")

        url = f"{self.base_url}/sports/{sport}/odds"
        params = {
            "api_key": self.api_key,
            "regions": regions,
            "markets": markets,
            "oddsFormat": odds_format,
            "dateFormat": date_format,
        }

        async with httpx.AsyncClient(timeout=20) as client:
            res = await client.get(url, params=params)
            res.raise_for_status()

            return {
                "data": res.json(),
                "usage": {
                    "requests_remaining": res.headers.get("x-requests-remaining"),
                    "requests_used": res.headers.get("x-requests-used"),
                },
            }

    @cached(ttl_seconds=60 * 60)
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
        regions: str = "us",
        markets: str = "player_points",
    ):
        logger.info("THE ODDS API CALLED: get_event_odds")

        async with httpx.AsyncClient(timeout=20) as client:
            res = await client.get(
                f"{self.base_url}/sports/{sport}/events/{event_id}/odds",
                params={
                    "api_key": self.api_key,
                    "regions": regions,
                    "markets": markets,
                },
            )
            res.raise_for_status()
            return res.json()
