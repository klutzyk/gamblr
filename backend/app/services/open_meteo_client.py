from __future__ import annotations

import logging
from typing import Any

import httpx

from app.core.config import settings


logger = logging.getLogger(__name__)


DEFAULT_HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "surface_pressure",
    "pressure_msl",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "cloud_cover",
    "visibility",
    "precipitation_probability",
    "precipitation",
    "rain",
    "showers",
    "snowfall",
    "weather_code",
]


class OpenMeteoClient:
    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout
        self.base_url = settings.OPEN_METEO_BASE_URL.rstrip("/")
        self.historical_base_url = settings.OPEN_METEO_HISTORICAL_BASE_URL.rstrip("/")

    @staticmethod
    def _clean_params(params: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in params.items()
            if value is not None and value != ""
        }

    async def _get_json(
        self,
        base_url: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], str]:
        url = f"{base_url}/{path.lstrip('/')}"
        cleaned_params = self._clean_params(params or {})

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(url, params=cleaned_params)
            response.raise_for_status()
            logger.info("Open-Meteo GET %s params=%s", url, cleaned_params)
            return response.json(), str(response.request.url)

    async def get_forecast(
        self,
        *,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        hourly: list[str] | None = None,
        timezone: str = "UTC",
        forecast_days: int | None = None,
        past_days: int | None = None,
    ) -> tuple[dict[str, Any], str]:
        return await self._get_json(
            self.base_url,
            "forecast",
            params={
                "latitude": latitude,
                "longitude": longitude,
                "hourly": ",".join(hourly or DEFAULT_HOURLY_VARIABLES),
                "timezone": timezone,
                "start_date": start_date,
                "end_date": end_date,
                "forecast_days": forecast_days,
                "past_days": past_days,
            },
        )

    async def get_historical_forecast(
        self,
        *,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        hourly: list[str] | None = None,
        timezone: str = "UTC",
    ) -> tuple[dict[str, Any], str]:
        return await self._get_json(
            self.historical_base_url,
            "forecast",
            params={
                "latitude": latitude,
                "longitude": longitude,
                "hourly": ",".join(hourly or DEFAULT_HOURLY_VARIABLES),
                "timezone": timezone,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
