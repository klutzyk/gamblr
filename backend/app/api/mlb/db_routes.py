import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.mlb.session import get_mlb_db
from app.db.mlb.store_ingestion import (
    bootstrap_mlb_ingestion,
    ingest_game_feed,
    ingest_game_feeds,
    ingest_savant_bat_tracking,
    ingest_savant_park_factors,
    ingest_savant_statcast_batters,
    ingest_savant_statcast_pitchers,
    ingest_savant_swing_path,
    ingest_schedule,
    ingest_teams,
)


logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/teams/load")
async def load_mlb_teams(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_teams(db, season=season)
    except Exception as exc:
        logger.exception("MLB team ingestion failed for season %s", season)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/schedule/load")
async def load_mlb_schedule(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    start_date: str | None = Query(None, description="Optional YYYY-MM-DD"),
    end_date: str | None = Query(None, description="Optional YYYY-MM-DD"),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_schedule(
            db,
            season=season,
            start_date=start_date,
            end_date=end_date,
        )
    except Exception as exc:
        logger.exception(
            "MLB schedule ingestion failed for season=%s start=%s end=%s",
            season,
            start_date,
            end_date,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/games/ingest")
async def ingest_mlb_games(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    since: str = Query(..., description="YYYY-MM-DD"),
    until: str = Query(..., description="YYYY-MM-DD"),
    final_only: bool = Query(False, description="Only ingest games already marked final"),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_game_feeds(
            db,
            season=season,
            start_date=since,
            end_date=until,
            final_only=final_only,
        )
    except Exception as exc:
        logger.exception(
            "MLB game ingestion failed for season=%s since=%s until=%s",
            season,
            since,
            until,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/game/{game_pk}/ingest")
async def ingest_single_mlb_game(
    game_pk: int,
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_game_feed(db, game_pk=game_pk)
    except Exception as exc:
        logger.exception("Single MLB game ingestion failed for game_pk=%s", game_pk)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/season/statcast-batters/load")
async def load_mlb_statcast_batters(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_savant_statcast_batters(db, season=season)
    except Exception as exc:
        logger.exception("MLB Savant batter ingest failed for season=%s", season)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/season/statcast-pitchers/load")
async def load_mlb_statcast_pitchers(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_savant_statcast_pitchers(db, season=season)
    except Exception as exc:
        logger.exception("MLB Savant pitcher ingest failed for season=%s", season)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/season/bat-tracking/load")
async def load_mlb_bat_tracking(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    min_swings: int = Query(100, ge=1),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_savant_bat_tracking(db, season=season, min_swings=min_swings)
    except Exception as exc:
        logger.exception("MLB bat-tracking ingest failed for season=%s", season)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/season/swing-path/load")
async def load_mlb_swing_path(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    min_swings: int = Query(100, ge=1),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_savant_swing_path(db, season=season, min_swings=min_swings)
    except Exception as exc:
        logger.exception("MLB swing-path ingest failed for season=%s", season)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/season/park-factors/load")
async def load_mlb_park_factors(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_savant_park_factors(db, season=season)
    except Exception as exc:
        logger.exception("MLB park-factor ingest failed for season=%s", season)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/bootstrap/load")
async def bootstrap_mlb_pipeline(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    since: str = Query(..., description="YYYY-MM-DD"),
    until: str = Query(..., description="YYYY-MM-DD"),
    final_only: bool = Query(False),
    include_savant: bool = Query(True),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await bootstrap_mlb_ingestion(
            db,
            season=season,
            start_date=since,
            end_date=until,
            final_only=final_only,
            include_savant=include_savant,
        )
    except Exception as exc:
        logger.exception(
            "MLB bootstrap failed for season=%s since=%s until=%s",
            season,
            since,
            until,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc
