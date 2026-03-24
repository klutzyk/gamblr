from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import create_engine
from app.core.config import settings
from app.db.url_utils import to_sync_db_url
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
    train_threept_model,
    train_threepa_model,
)
from app.db.store_prediction_logs import update_prediction_actuals, delete_walkforward_logs
from app.db.store_first_basket import update_first_basket_actuals
from ml.backtest import walk_forward_backtest
from app.db.store_prediction_logs import log_predictions
from ml.first_basket_labels import build_first_basket_labels
from ml.first_basket_model import train_first_basket_models
from ml.under_side_model import train_under_side_model

router = APIRouter()

sync_engine = create_engine(to_sync_db_url(settings.DATABASE_URL))


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


@router.post("/train/threept")
async def train_threept():
    results = await run_in_threadpool(train_threept_model, sync_engine)
    return {"status": "trained", **results}


@router.post("/train/threepa")
async def train_threepa():
    results = await run_in_threadpool(train_threepa_model, sync_engine)
    return {"status": "trained", **results}


@router.post("/train/all")
async def train_all():
    minutes = await run_in_threadpool(train_minutes_model, sync_engine)
    points = await run_in_threadpool(train_points_model, sync_engine)
    assists = await run_in_threadpool(train_assists_model, sync_engine)
    rebounds = await run_in_threadpool(train_rebounds_model, sync_engine)
    threept = await run_in_threadpool(train_threept_model, sync_engine)
    threepa = await run_in_threadpool(train_threepa_model, sync_engine)
    try:
        under_side = await run_in_threadpool(
            train_under_side_model,
            sync_engine,
            180,
            300,
        )
    except Exception as e:
        under_side = {"status": "error", "detail": str(e)}
    return {
        "status": "trained",
        "minutes": minutes,
        "points": points,
        "assists": assists,
        "rebounds": rebounds,
        "threept": threept,
        "threepa": threepa,
        "under_side": under_side,
    }


@router.post("/evaluate/all")
async def evaluate_all():
    points = await run_in_threadpool(update_prediction_actuals, sync_engine, "points")
    assists = await run_in_threadpool(update_prediction_actuals, sync_engine, "assists")
    rebounds = await run_in_threadpool(update_prediction_actuals, sync_engine, "rebounds")
    minutes = await run_in_threadpool(update_prediction_actuals, sync_engine, "minutes")
    threept = await run_in_threadpool(update_prediction_actuals, sync_engine, "threept")
    threepa = await run_in_threadpool(update_prediction_actuals, sync_engine, "threepa")
    return {
        "status": "updated",
        "points": points,
        "assists": assists,
        "rebounds": rebounds,
        "minutes": minutes,
        "threept": threept,
        "threepa": threepa,
    }


@router.post("/backtest/walkforward/{stat_type}")
async def backtest_walkforward(
    stat_type: str,
    min_games: int = 15,
    max_dates: int | None = None,
    reset: bool = False,
):
    if reset:
        await run_in_threadpool(delete_walkforward_logs, sync_engine, stat_type)
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
        True,
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


@router.post("/first-basket/build-labels")
async def build_first_basket_labels_api(
    season: str | None = None,
    max_games: int | None = 150,
    overwrite: bool = False,
    timeout: int = 12,
):
    result = await run_in_threadpool(
        build_first_basket_labels,
        sync_engine,
        season,
        max_games,
        overwrite,
        timeout,
    )
    return {"status": "ok", **result}


@router.post("/first-basket/train")
async def train_first_basket():
    result = await run_in_threadpool(train_first_basket_models, sync_engine)
    return {"status": "trained", **result}


@router.post("/first-basket/evaluate")
async def evaluate_first_basket():
    updated = await run_in_threadpool(update_first_basket_actuals, sync_engine)
    return {"status": "updated", "rows_updated": updated}


@router.post("/under-side/train")
async def train_under_side(
    lookback_days: int = 180,
    min_rows_per_stat: int = 300,
):
    result = await run_in_threadpool(
        train_under_side_model,
        sync_engine,
        lookback_days,
        min_rows_per_stat,
    )
    return {"status": "trained", **result}
