from __future__ import annotations

import logging
from typing import Any

import httpx


logger = logging.getLogger(__name__)


class MlbStatsApiClient:
    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout
        self.base_url = "https://statsapi.mlb.com/api/v1"
        self.feed_base_url = "https://statsapi.mlb.com/api/v1.1"

    @staticmethod
    def _clean_params(params: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in params.items()
            if value is not None and value != ""
        }

    async def _get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        feed_version: bool = False,
    ) -> tuple[dict[str, Any], str]:
        base_url = self.feed_base_url if feed_version else self.base_url
        url = f"{base_url}/{path.lstrip('/')}"
        cleaned_params = self._clean_params(params or {})

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(url, params=cleaned_params)
            response.raise_for_status()
            logger.info("MLB StatsAPI GET %s params=%s", url, cleaned_params)
            return response.json(), str(response.request.url)

    async def get_teams(
        self,
        *,
        season: int,
        sport_id: int = 1,
        active_status: str = "Y",
        hydrate: str | None = None,
    ) -> tuple[dict[str, Any], str]:
        return await self._get_json(
            "teams",
            params={
                "season": season,
                "sportId": sport_id,
                "activeStatus": active_status,
                "hydrate": hydrate,
            },
        )

    async def get_venues(
        self,
        *,
        venue_ids: list[int],
        season: int | None = None,
    ) -> tuple[dict[str, Any], str]:
        if not venue_ids:
            return {"venues": []}, f"{self.base_url}/venues"
        return await self._get_json(
            "venues",
            params={
                "venueIds": ",".join(str(value) for value in venue_ids),
                "season": season,
            },
        )

    async def get_schedule(
        self,
        *,
        season: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        game_types: str | None = None,
        sport_id: int = 1,
        hydrate: str | None = None,
    ) -> tuple[dict[str, Any], str]:
        return await self._get_json(
            "schedule",
            params={
                "season": season,
                "startDate": start_date,
                "endDate": end_date,
                "gameTypes": game_types,
                "sportId": sport_id,
                "hydrate": hydrate,
            },
        )

    async def get_game_feed(
        self,
        *,
        game_pk: int,
        timecode: str | None = None,
        hydrate: str | None = None,
    ) -> tuple[dict[str, Any], str]:
        return await self._get_json(
            f"game/{game_pk}/feed/live",
            params={
                "timecode": timecode,
                "hydrate": hydrate,
            },
            feed_version=True,
        )
