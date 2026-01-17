from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    DATABASE_URL: str
    ML_DATABASE_URL: str

    # sportsdata.io
    SPORTSDATA_API_KEY: str
    SPORTSDATA_BASE_URL: str = "https://api.sportsdata.io/v3/nba"

    # the-odds-api.com
    THEODDS_BASE_URL: str
    THEODDS_API_KEY: str

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
    )


settings = Settings()
