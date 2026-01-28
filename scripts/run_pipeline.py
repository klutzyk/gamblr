import sys
from datetime import datetime
from pathlib import Path

import httpx
from sqlalchemy import create_engine


ROOT_DIR = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT_DIR / "backend"
ML_DIR = ROOT_DIR / "ml"

sys.path.insert(0, str(BACKEND_DIR))
sys.path.insert(0, str(ML_DIR))

from app.core.config import settings  # noqa: E402
from update_rolling import update_rolling_stats  # noqa: E402


def prompt(text: str, default: str | None = None) -> str:
    hint = f" [{default}]" if default is not None else ""
    value = input(f"{text}{hint}: ").strip()
    return value or (default or "")


def prompt_yes_no(text: str, default: bool = False) -> bool:
    suffix = "Y/n" if default else "y/N"
    value = input(f"{text} ({suffix}): ").strip().lower()
    if not value:
        return default
    return value in {"y", "yes"}


def call_api(client: httpx.Client, method: str, path: str, params: dict | None = None):
    print(f"-> {method} {path} {params or ''}".strip())
    response = client.request(method, path, params=params, timeout=600)
    response.raise_for_status()
    data = response.json()
    print(f"   done: {data}")
    return data


def main():
    print("Gamblr pipeline runner")
    base_url = prompt("API base URL", "http://127.0.0.1:8000")
    since_date = prompt("Ingest since date (YYYY-MM-DD, empty to skip)", "")
    backfill_shooting = prompt_yes_no("Backfill missing shooting stats", True)
    run_backtests = prompt_yes_no("Run backtests (assists/rebounds/threept)", False)

    if since_date:
        try:
            datetime.strptime(since_date, "%Y-%m-%d")
        except ValueError as exc:
            raise SystemExit("Invalid date format, expected YYYY-MM-DD.") from exc

    with httpx.Client(base_url=base_url) as client:
        if since_date:
            print("Ingesting player game logs since date...")
            call_api(client, "POST", "/db/last-n/update", {"since": since_date})
            print("Ingesting team game logs since date...")
            call_api(client, "POST", "/db/team-games/update")
        else:
            print("Skipping ingest step.")

        if backfill_shooting:
            print("Backfilling player shooting stats (missing columns)...")
            call_api(client, "POST", "/db/last-n/backfill-shooting")
            print("Backfilling team shooting stats (missing columns)...")
            call_api(client, "POST", "/db/team-games/backfill-shooting")

    print("Updating rolling features CSV...")
    engine = create_engine(settings.DATABASE_URL.replace("+asyncpg", ""))
    update_rolling_stats(engine)
    print("Rolling features updated.")

    with httpx.Client(base_url=base_url) as client:
        print("Training models...")
        call_api(client, "POST", "/ml/train/all")

        if run_backtests:
            print("Running backtests...")
            for stat in ("assists", "rebounds", "threept"):
                call_api(
                    client,
                    "POST",
                    f"/ml/backtest/walkforward/{stat}",
                    {"reset": "true"},
                )

    print("Pipeline complete.")


if __name__ == "__main__":
    main()
