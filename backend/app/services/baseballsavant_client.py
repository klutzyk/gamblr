from __future__ import annotations

import logging
import json
import re
from io import StringIO
from typing import Any

import httpx
import pandas as pd
from pandas.errors import ParserError


logger = logging.getLogger(__name__)


BATTER_CUSTOM_SELECTIONS = [
    "pa",
    "k_percent",
    "bb_percent",
    "xba",
    "xslg",
    "xwoba",
    "xobp",
    "xiso",
    "exit_velocity_avg",
    "launch_angle_avg",
    "sweet_spot_percent",
    "barrel_batted_rate",
    "hard_hit_percent",
]

PITCHER_CUSTOM_SELECTIONS = [
    "k_percent",
    "bb_percent",
    "xera",
    "xba",
    "xslg",
    "xwoba",
    "exit_velocity_avg",
    "launch_angle_avg",
    "barrel_batted_rate",
    "hard_hit_percent",
]


class BaseballSavantClient:
    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout
        self.base_url = "https://baseballsavant.mlb.com"

    @staticmethod
    def _clean_params(params: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in params.items()
            if value is not None and value != ""
        }

    async def _fetch_text(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> tuple[str, str]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        cleaned_params = self._clean_params(params or {})

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(url, params=cleaned_params)
            response.raise_for_status()
            logger.info("Baseball Savant GET %s params=%s", url, cleaned_params)
            return response.text, str(response.request.url)

    async def _fetch_csv(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> tuple[pd.DataFrame, str, str]:
        text, request_url = await self._fetch_text(path, params=params)
        try:
            dataframe = pd.read_csv(StringIO(text))
        except ParserError:
            if "statcast-park-factors" not in path:
                raise
            match = re.search(r"var data = (\[.*?\]);", text, re.DOTALL)
            if not match:
                raise
            dataframe = pd.DataFrame(json.loads(match.group(1)))
        return dataframe, text, request_url

    async def get_custom_leaderboard(
        self,
        *,
        season: int,
        player_type: str,
        selections: list[str] | None = None,
        minimum: str = "q",
    ) -> tuple[pd.DataFrame, str, str]:
        chosen = selections or (
            BATTER_CUSTOM_SELECTIONS if player_type == "batter" else PITCHER_CUSTOM_SELECTIONS
        )
        first_metric = chosen[0]
        return await self._fetch_csv(
            "leaderboard/custom",
            params={
                "csv": "true",
                "chart": "false",
                "chartType": "beeswarm",
                "min": minimum,
                "r": "no",
                "selections": ",".join(chosen),
                "sort": first_metric,
                "sortDir": "desc",
                "type": player_type,
                "x": first_metric,
                "y": first_metric,
                "year": season,
            },
        )

    async def get_bat_tracking(
        self,
        *,
        season: int,
        player_type: str = "batter",
        min_swings: int = 0,
        game_type: str = "Regular",
    ) -> tuple[pd.DataFrame, str, str]:
        return await self._fetch_csv(
            "leaderboard/bat-tracking",
            params={
                "csv": "true",
                "seasonStart": season,
                "seasonEnd": season,
                "type": player_type,
                "gameType": game_type,
                "minSwings": min_swings,
                "minGroupSwings": 1,
            },
        )

    async def get_swing_path(
        self,
        *,
        season: int,
        player_type: str = "batter",
        min_swings: int = 0,
        game_type: str = "Regular",
    ) -> tuple[pd.DataFrame, str, str]:
        return await self._fetch_csv(
            "leaderboard/bat-tracking/swing-path-attack-angle",
            params={
                "csv": "true",
                "seasonStart": season,
                "seasonEnd": season,
                "type": player_type,
                "gameType": game_type,
                "minSwings": min_swings,
                "minGroupSwings": 1,
            },
        )

    async def get_park_factors(
        self,
        *,
        season: int,
        stat: str = "index_HR",
        factor_type: str = "year",
        bat_side: str = "",
        condition: str = "All",
        rolling: str = "",
        parks: str = "mlb",
    ) -> tuple[pd.DataFrame, str, str]:
        return await self._fetch_csv(
            "leaderboard/statcast-park-factors",
            params={
                "csv": "true",
                "year": season,
                "stat": stat,
                "type": factor_type,
                "batSide": bat_side,
                "condition": condition,
                "rolling": rolling,
                "parks": parks,
            },
        )
