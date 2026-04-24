from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    DATABASE_URL: str
    ML_DATABASE_URL: str
    OPEN_METEO_BASE_URL: str = "https://api.open-meteo.com/v1"
    OPEN_METEO_HISTORICAL_BASE_URL: str = "https://historical-forecast-api.open-meteo.com/v1"

    # sportsdata.io
    SPORTSDATA_API_KEY: str
    SPORTSDATA_BASE_URL: str = "https://api.sportsdata.io/v3/nba"

    # the-odds-api.com
    THEODDS_BASE_URL: str
    THEODDS_API_KEY: str

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
