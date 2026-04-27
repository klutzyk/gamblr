from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from app.core.config import settings
from app.services.cache import cached


logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


class PropLineClient:
    def __init__(self):
        self.base_url = settings.PROPLINE_BASE_URL.rstrip("/")
        self.api_key = settings.PROPLINE_API_KEY
        if not self.api_key:
            raise ValueError("Missing PROPLINE_API_KEY in .env")

    @staticmethod
    def _clean_params(params: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in params.items() if value not in (None, "", [])}

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        query = self._clean_params({"apiKey": self.api_key, **(params or {})})
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(f"{self.base_url}/{path.lstrip('/')}", params=query)
            response.raise_for_status()
            logger.info("PropLine GET %s params=%s", path, {**query, "apiKey": "***"})
            return response.json()

    @cached(ttl_seconds=60)
    async def get_events(self, sport: str = "baseball_mlb") -> list[dict[str, Any]]:
        return await self._get(f"sports/{sport}/events")

    @cached(ttl_seconds=60)
    async def get_event_odds(
        self,
        *,
        sport: str = "baseball_mlb",
        event_id: str,
        markets: list[str] | str,
        bookmakers: list[str] | str | None = None,
        odds_format: str = "american",
    ) -> dict[str, Any]:
        market_value = ",".join(markets) if isinstance(markets, list) else markets
        bookmaker_value = ",".join(bookmakers) if isinstance(bookmakers, list) else bookmakers
        return await self._get(
            f"sports/{sport}/events/{event_id}/odds",
            params={
                "markets": market_value,
                "bookmakers": bookmaker_value,
                "oddsFormat": odds_format,
            },
        )

    async def get_market_odds_for_events(
        self,
        *,
        sport: str = "baseball_mlb",
        markets: list[str] | str = "batter_home_runs",
        bookmakers: list[str] | str | None = None,
        odds_format: str = "american",
        max_events: int | None = None,
        event_date: str | date | None = None,
    ) -> list[dict[str, Any]]:
        events = await self.get_events(sport)
        if event_date is not None:
            date_text = event_date.isoformat() if isinstance(event_date, date) else str(event_date)
            filtered_events = []
            for event in events:
                commence_time = str(event.get("commence_time") or event.get("commenceTime") or "")
                try:
                    event_dt = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
                    event_mlb_date = event_dt.astimezone(ZoneInfo("America/New_York")).date().isoformat()
                except ValueError:
                    event_mlb_date = commence_time[:10]
                if event_mlb_date == date_text:
                    filtered_events.append(event)
            events = filtered_events
        if max_events is not None:
            events = events[:max_events]

        results = []
        for event in events:
            event_id = str(event.get("id") or event.get("event_id") or "")
            if not event_id:
                continue
            try:
                odds = await self.get_event_odds(
                    sport=sport,
                    event_id=event_id,
                    markets=markets,
                    bookmakers=bookmakers,
                    odds_format=odds_format,
                )
            except httpx.HTTPStatusError as exc:
                logger.warning("PropLine odds failed for event_id=%s: %s", event_id, exc)
                continue
            results.append({"event": event, "odds": odds})
        return results
