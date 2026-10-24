from __future__ import annotations

import asyncio
import logging
import os
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
    def __init__(self, timeout: float = 30.0, retries: int | None = None):
        self.timeout = timeout
        self.retries = retries if retries is not None else int(os.getenv("OPEN_METEO_RETRIES", "3"))
        self.rate_limit_backoff_seconds = float(os.getenv("OPEN_METEO_429_BACKOFF_SECONDS", "10"))
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
            last_error: Exception | None = None
            for attempt in range(self.retries + 1):
                try:
                    response = await client.get(url, params=cleaned_params)
                    response.raise_for_status()
                    logger.info("Open-Meteo GET %s params=%s", url, cleaned_params)
                    return response.json(), str(response.request.url)
                except httpx.HTTPStatusError as exc:
                    last_error = exc
                    if exc.response.status_code < 500 and exc.response.status_code != 429:
                        raise
                except httpx.RequestError as exc:
                    last_error = exc
                if attempt < self.retries:
                    status_code = (
                        last_error.response.status_code
                        if isinstance(last_error, httpx.HTTPStatusError)
                        else None
                    )
                    delay = (
                        self.rate_limit_backoff_seconds * (attempt + 1)
                        if status_code == 429
                        else 0.75 * (attempt + 1)
                    )
                    await asyncio.sleep(delay)
            if last_error:
                raise last_error
            raise RuntimeError("Open-Meteo request failed without an exception.")

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
