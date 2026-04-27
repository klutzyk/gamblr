from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import create_engine

from app.core.config import settings
from app.db.url_utils import to_sync_db_url
from app.services.mlb_simulation import list_simulation_games, run_game_simulation


router = APIRouter()


def _engine():
    return create_engine(to_sync_db_url(settings.ML_DATABASE_URL))


@router.get("/games")
async def get_mlb_simulation_games(
    date: str = Query(..., description="YYYY-MM-DD slate date."),
):
    try:
        return await run_in_threadpool(list_simulation_games, _engine(), target_date=date)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/game/{game_pk}/run")
async def run_mlb_game_simulation(
    game_pk: int,
    iterations: int = Query(250, ge=1, le=2000),
    seed: int | None = Query(None),
    pitch_log_limit: int = Query(700, ge=100, le=1400),
):
    try:
        return await run_in_threadpool(
            run_game_simulation,
            _engine(),
            game_pk=game_pk,
            iterations=iterations,
            seed=seed,
            pitch_log_limit=pitch_log_limit,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
