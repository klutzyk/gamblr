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
