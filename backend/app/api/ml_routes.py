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

from ml.training import train_points_model

router = APIRouter()

sync_engine = create_engine(settings.DATABASE_URL.replace("+asyncpg", ""))


@router.post("/train/points")
async def train_points():
    results = await run_in_threadpool(train_points_model, sync_engine)
    return {"status": "trained", **results}
