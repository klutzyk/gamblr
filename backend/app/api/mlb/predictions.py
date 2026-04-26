import sys
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import create_engine

from app.core.config import settings
from app.db.url_utils import to_sync_db_url


ROOT_DIR = Path(__file__).resolve().parents[4]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from ml.mlb.artifacts import (  # noqa: E402
    all_market_statuses,
    latest_market_report,
    list_market_reports,
    market_names,
    market_status,
)
from ml.mlb.evaluate import score_completed_games  # noqa: E402
from ml.mlb.training import train_all as train_all_mlb_markets  # noqa: E402
from ml.mlb.training import train_market  # noqa: E402

router = APIRouter()

PLANNED_MARKETS = market_names()


def _sync_engine():
    return create_engine(to_sync_db_url(settings.ML_DATABASE_URL))


@router.get("/markets")
def get_mlb_markets():
    return {
        "sport": "mlb",
        "status": "training_ready",
        "markets": all_market_statuses(),
    }


@router.get("/models/{market}/latest")
def get_latest_mlb_model(market: str):
    if market not in PLANNED_MARKETS:
        raise HTTPException(status_code=404, detail="Unknown MLB market.")
    return {
        "sport": "mlb",
        "status": market_status(market),
    }


@router.get("/reports/{market}")
def get_mlb_model_reports(
    market: str,
    limit: int = Query(5, ge=1, le=50),
):
    if market not in PLANNED_MARKETS:
        raise HTTPException(status_code=404, detail="Unknown MLB market.")
    return {
        "sport": "mlb",
        "market": market,
        "reports": list_market_reports(market, limit=limit),
    }


@router.post("/train/all")
async def train_all_mlb(
    min_player_games: int = Query(3, ge=0, le=100),
):
    try:
        reports = await run_in_threadpool(
            train_all_mlb_markets,
            database_url=to_sync_db_url(settings.ML_DATABASE_URL),
            min_player_games=min_player_games,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"sport": "mlb", "status": "trained", "reports": reports}


@router.post("/train/{market}")
async def train_mlb_market(
    market: str,
    min_player_games: int = Query(3, ge=0, le=100),
):
    if market not in PLANNED_MARKETS:
        raise HTTPException(status_code=404, detail="Unknown MLB market.")
    try:
        report = await run_in_threadpool(
            train_market,
            market,
            engine=_sync_engine(),
            min_player_games=min_player_games,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"sport": "mlb", "status": "trained", "market": market, "report": report}


@router.get("/evaluate/{market}")
async def evaluate_mlb_market(
    market: str,
    since: str | None = Query(None, description="Inclusive YYYY-MM-DD lower bound."),
    until: str | None = Query(None, description="Inclusive YYYY-MM-DD upper bound."),
    last_days: int | None = Query(30, ge=1, le=365),
    limit: int = Query(25, ge=1, le=200),
):
    if market not in PLANNED_MARKETS:
        raise HTTPException(status_code=404, detail="Unknown MLB market.")
    try:
        result = await run_in_threadpool(
            score_completed_games,
            market,
            database_url=to_sync_db_url(settings.ML_DATABASE_URL),
            since=since,
            until=until,
            last_days=last_days,
            limit=limit,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"sport": "mlb", "status": "evaluated", **result}


@router.get("/{market}")
def get_mlb_predictions(
    market: str,
    day: str = Query("today", enum=["today", "tomorrow", "yesterday", "auto"]),
):
    if market not in PLANNED_MARKETS:
        raise HTTPException(status_code=404, detail="Unknown MLB market.")

    report = latest_market_report(market)
    raise HTTPException(
        status_code=501,
        detail={
            "sport": "mlb",
            "market": market,
            "day": day,
            "status": "pregame_scoring_frame_not_implemented",
            "trained": report is not None,
            "latest_report_path": report.get("report_path") if report else None,
            "message": (
                "Training and model artifact endpoints are wired. Live MLB predictions need "
                "the next feature-frame step: build rows from scheduled games, lineup snapshots, "
                "probable starters, weather, park factors, and prior-only rolling stats."
            ),
        },
    )
