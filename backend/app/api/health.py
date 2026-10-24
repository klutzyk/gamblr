# Health check endpoint
from fastapi import APIRouter
from sqlalchemy import text

from app.core.config import settings
from app.db.session import AsyncSessionLocal

router = APIRouter()

@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/health/dependencies")
async def dependency_health():
    database = "ok"
    detail = None
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
    except Exception as exc:
        database = "unavailable"
        detail = f"{type(exc).__name__}: database connection failed"
    return {
        "status": "ok" if database == "ok" else "degraded",
        "database": database,
        "database_detail": detail,
        "optional_integrations": {
            "the_odds_api": bool(settings.THEODDS_API_KEY),
            "sportsdata": bool(settings.SPORTSDATA_API_KEY),
            "propline": bool(settings.PROPLINE_API_KEY),
        },
    }
