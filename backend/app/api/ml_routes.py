from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import create_engine
from app.core.config import settings
import sys
from pathlib import Path

# get the path to the project root
ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from ml.training import (
    train_points_model,
    train_assists_model,
    train_rebounds_model,
    train_minutes_model,
)
from app.db.store_prediction_logs import update_prediction_actuals
from ml.backtest import walk_forward_backtest
from app.db.store_prediction_logs import log_predictions

router = APIRouter()

sync_engine = create_engine(settings.DATABASE_URL.replace("+asyncpg", ""))


@router.post("/train/points")
async def train_points():
    results = await run_in_threadpool(train_points_model, sync_engine)
    return {"status": "trained", **results}


@router.post("/train/assists")
async def train_assists():
    results = await run_in_threadpool(train_assists_model, sync_engine)
    return {"status": "trained", **results}


@router.post("/train/rebounds")
async def train_rebounds():
    results = await run_in_threadpool(train_rebounds_model, sync_engine)
    return {"status": "trained", **results}


@router.post("/train/minutes")
async def train_minutes():
    results = await run_in_threadpool(train_minutes_model, sync_engine)
    return {"status": "trained", **results}


@router.post("/train/all")
async def train_all():
    minutes = await run_in_threadpool(train_minutes_model, sync_engine)
    points = await run_in_threadpool(train_points_model, sync_engine)
    assists = await run_in_threadpool(train_assists_model, sync_engine)
    rebounds = await run_in_threadpool(train_rebounds_model, sync_engine)
    return {
        "status": "trained",
        "minutes": minutes,
        "points": points,
        "assists": assists,
        "rebounds": rebounds,
    }


@router.post("/evaluate/all")
async def evaluate_all():
    points = await run_in_threadpool(update_prediction_actuals, sync_engine, "points")
    assists = await run_in_threadpool(update_prediction_actuals, sync_engine, "assists")
    rebounds = await run_in_threadpool(update_prediction_actuals, sync_engine, "rebounds")
    minutes = await run_in_threadpool(update_prediction_actuals, sync_engine, "minutes")
    return {
        "status": "updated",
        "points": points,
        "assists": assists,
        "rebounds": rebounds,
        "minutes": minutes,
    }


@router.post("/backtest/walkforward/{stat_type}")
async def backtest_walkforward(
    stat_type: str,
    min_games: int = 15,
    max_dates: int | None = None,
):
    df_preds = await run_in_threadpool(
        walk_forward_backtest, sync_engine, stat_type, min_games, max_dates
    )
    if df_preds.empty:
        return {"status": "no_data"}

    await run_in_threadpool(
        log_predictions,
        sync_engine,
        df_preds,
        stat_type,
        "walkforward",
        False,
    )
    await run_in_threadpool(update_prediction_actuals, sync_engine, stat_type)
    return {
        "status": "logged",
        "rows": int(len(df_preds)),
    }


@router.post("/evaluate/{stat_type}")
async def evaluate_stat(stat_type: str):
    updated = await run_in_threadpool(update_prediction_actuals, sync_engine, stat_type)
    return {"status": "updated", "stat_type": stat_type, "rows_updated": updated}
