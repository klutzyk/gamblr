import sys
from datetime import datetime, timedelta
from pathlib import Path
import time

import httpx
from sqlalchemy import create_engine
from sqlalchemy import text


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


def call_api(
    client: httpx.Client,
    method: str,
    path: str,
    params: dict | None = None,
    timeout_seconds: int = 600,
):
    print(f"-> {method} {path} {params or ''}".strip())
    response = client.request(method, path, params=params, timeout=timeout_seconds)
    response.raise_for_status()
    data = response.json()
    print(f"   done: {data}")
    return data


def call_api_with_retry(
    client: httpx.Client,
    method: str,
    path: str,
    params: dict | None = None,
    timeout_seconds: int = 600,
    retries: int = 3,
    retry_sleep: int = 5,
):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return call_api(client, method, path, params, timeout_seconds)
        except (httpx.ReadTimeout, httpx.RequestError) as exc:
            last_error = exc
            if attempt >= retries:
                break
            print(
                f"   warning: {type(exc).__name__} on {method} {path} "
                f"(attempt {attempt}/{retries}), retrying in {retry_sleep}s..."
            )
            time.sleep(retry_sleep)
    raise last_error


def get_last_ingest_date(engine) -> str | None:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT MAX(since_date) AS last_date
                    FROM ingestion_runs
                    WHERE ingest_type = 'last_n_update'
                      AND status = 'completed'
                    """
                )
            ).fetchone()
        if row and row[0]:
            return str(row[0])
    except Exception:
        return None
    return None


def main():
    print("Gamblr pipeline runner")
    base_url = prompt("API base URL", "http://127.0.0.1:8000")
    engine = create_engine(settings.DATABASE_URL.replace("+asyncpg", ""))
    last_ingest = get_last_ingest_date(engine)
    default_since = ""
    if last_ingest:
        try:
            next_date = datetime.strptime(last_ingest, "%Y-%m-%d").date() + timedelta(days=1)
            default_since = str(next_date)
        except ValueError:
            default_since = ""
    if last_ingest:
        print(f"Last ingest date: {last_ingest}")
    since_date = prompt("Ingest since date (YYYY-MM-DD, empty to skip)", default_since)
    update_actuals = prompt_yes_no("Update prediction actuals (/ml/evaluate/all)", True)
    recalc_under_risk = prompt_yes_no("Recalculate under-risk metrics", True)
    run_backtests = prompt_yes_no("Run backtests (assists/rebounds/threept)", False)

    if since_date:
        try:
            datetime.strptime(since_date, "%Y-%m-%d")
        except ValueError as exc:
            raise SystemExit("Invalid date format, expected YYYY-MM-DD.") from exc

    with httpx.Client(base_url=base_url) as client:
        if since_date:
            print("Ingesting player game logs since date...")
            start = call_api(
                client,
                "POST",
                "/db/last-n/update/start",
                {"since": since_date},
            )
            job_id = start["job_id"]
            print(f"Polling ingest job {job_id}...")
            while True:
                status = call_api_with_retry(
                    client,
                    "GET",
                    f"/db/jobs/{job_id}",
                    timeout_seconds=120,
                    retries=5,
                    retry_sleep=6,
                )
                state = status.get("status")
                done = status.get("players_done")
                total = status.get("players_total")
                if total:
                    print(f"   progress: {done}/{total}")
                else:
                    print(f"   status: {state}")

                if state == "completed":
                    print("Ingest job completed.")
                    break
                if state == "failed":
                    raise SystemExit(f"Ingest job failed: {status.get('error')}")

                time.sleep(20)
            print("Ingesting team game logs since date...")
            call_api(client, "POST", "/db/team-games/update")
        else:
            print("Skipping ingest step.")

        if update_actuals:
            print("Updating prediction actuals...")
            call_api(client, "POST", "/ml/evaluate/all")

    print("Updating rolling features CSV...")
    update_rolling_stats(engine)
    print("Rolling features updated.")

    with httpx.Client(base_url=base_url) as client:
        if recalc_under_risk:
            print("Recalculating under-risk metrics...")
            call_api(client, "POST", "/db/under-risk/recalc-all")

        print("Training models...")
        call_api(client, "POST", "/ml/train/all")

        if run_backtests:
            print("Running backtests...")
            for stat in ("points", "assists", "rebounds", "threept"):
                call_api(
                    client,
                    "POST",
                    f"/ml/backtest/walkforward/{stat}",
                    {"reset": "false"},
                )

    print("Pipeline complete.")


if __name__ == "__main__":
    main()
