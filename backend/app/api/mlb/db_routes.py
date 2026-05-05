import asyncio
import logging
import uuid
from datetime import date as date_type
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.mlb.session import MlbAsyncSessionLocal, get_mlb_db
from app.db.mlb.store_ingestion import (
    bootstrap_mlb_ingestion,
    ingest_active_rosters,
    ingest_context_window,
    ingest_game_feed,
    ingest_game_feeds,
    ingest_team_roster,
    ingest_umpire_roster,
    ingest_savant_bat_tracking,
    ingest_savant_park_factors,
    ingest_savant_season_bundle,
    ingest_savant_statcast_batters,
    ingest_savant_statcast_pitchers,
    ingest_savant_swing_path,
    ingest_schedule,
    ingest_teams,
    ingest_weather_for_game,
    ingest_weather_for_games,
)


logger = logging.getLogger(__name__)
router = APIRouter()
mlb_ingest_jobs: dict[str, dict] = {}


def _validate_yyyy_mm_dd(value: str, field_name: str) -> str:
    normalized = str(value).strip()
    try:
        datetime.strptime(normalized, "%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{field_name} must be YYYY-MM-DD") from exc
    return normalized


def _parse_yyyy_mm_dd_date(value: str, field_name: str) -> date_type:
    normalized = _validate_yyyy_mm_dd(value, field_name)
    return datetime.strptime(normalized, "%Y-%m-%d").date()


def _current_mlb_date():
    return datetime.now(ZoneInfo("America/New_York")).date()


async def _run_mlb_bootstrap_ingest_job(
    job_id: str,
    *,
    season: int,
    since: str,
    until: str,
    final_only: bool,
    include_savant: bool,
    include_weather: bool,
    include_umpire_roster: bool,
    weather_dataset: str,
    statcast_minimum: str,
    bat_tracking_min_swings: int,
) -> None:
    job = mlb_ingest_jobs[job_id]
    job["status"] = "running"
    job["started_at"] = datetime.utcnow().isoformat()
    try:
        async with MlbAsyncSessionLocal() as db:
            result = await bootstrap_mlb_ingestion(
                db,
                season=season,
                start_date=since,
                end_date=until,
                final_only=final_only,
                include_savant=include_savant,
                include_weather=include_weather,
                include_umpire_roster=include_umpire_roster,
                weather_dataset=weather_dataset,
                statcast_minimum=statcast_minimum,
                bat_tracking_min_swings=bat_tracking_min_swings,
            )
        job["status"] = "completed"
        job["result"] = result
        job["finished_at"] = datetime.utcnow().isoformat()
    except Exception as exc:
        logger.exception("MLB bootstrap ingest job %s failed", job_id)
        job["status"] = "failed"
        job["error"] = str(exc)
        job["finished_at"] = datetime.utcnow().isoformat()


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


@router.post("/rosters/team/{team_id}/load")
async def load_mlb_team_roster(
    team_id: int,
    roster_date: str = Query(..., alias="date", description="YYYY-MM-DD"),
    season: int | None = Query(None, description="MLB season year, e.g. 2026"),
    roster_type: str = Query("active", description="active, 40Man, depthChart, etc."),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_team_roster(
            db,
            team_id=team_id,
            roster_date=roster_date,
            season=season,
            roster_type=roster_type,
        )
    except Exception as exc:
        logger.exception("MLB team roster ingest failed for team_id=%s date=%s", team_id, roster_date)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/rosters/active/load")
async def load_mlb_active_rosters(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    roster_date: str = Query(..., alias="date", description="YYYY-MM-DD"),
    roster_type: str = Query("active", description="Usually active"),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_active_rosters(
            db,
            season=season,
            roster_date=roster_date,
            roster_type=roster_type,
        )
    except Exception as exc:
        logger.exception("MLB active roster ingest failed for season=%s date=%s", season, roster_date)
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


@router.post("/umpires/load")
async def load_mlb_umpires(
    date_value: str = Query(..., alias="date", description="YYYY-MM-DD"),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_umpire_roster(db, date_value=date_value)
    except Exception as exc:
        logger.exception("MLB umpire ingest failed for date=%s", date_value)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/weather/game/{game_pk}/load")
async def load_mlb_game_weather(
    game_pk: int,
    dataset: str = Query("auto", description="auto, forecast, or historical_forecast"),
    hours_before: int = Query(6, ge=0, le=48),
    hours_after: int = Query(6, ge=0, le=48),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_weather_for_game(
            db,
            game_pk=game_pk,
            dataset=dataset,
            hours_before=hours_before,
            hours_after=hours_after,
        )
    except Exception as exc:
        logger.exception("MLB weather ingest failed for game_pk=%s", game_pk)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/weather/load")
async def load_mlb_weather_window(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    dataset: str = Query("auto", description="auto, forecast, or historical_forecast"),
    hours_before: int = Query(6, ge=0, le=48),
    hours_after: int = Query(6, ge=0, le=48),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_weather_for_games(
            db,
            season=season,
            start_date=start_date,
            end_date=end_date,
            dataset=dataset,
            hours_before=hours_before,
            hours_after=hours_after,
        )
    except Exception as exc:
        logger.exception(
            "MLB weather window ingest failed for season=%s start=%s end=%s",
            season,
            start_date,
            end_date,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/context/load")
async def load_mlb_context_window(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    final_only: bool = Query(False),
    include_weather: bool = Query(True),
    weather_dataset: str = Query("auto", description="auto, forecast, or historical_forecast"),
    weather_hours_before: int = Query(6, ge=0, le=48),
    weather_hours_after: int = Query(6, ge=0, le=48),
    include_umpire_roster: bool = Query(True),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_context_window(
            db,
            season=season,
            start_date=start_date,
            end_date=end_date,
            final_only=final_only,
            include_weather=include_weather,
            weather_dataset=weather_dataset,
            weather_hours_before=weather_hours_before,
            weather_hours_after=weather_hours_after,
            include_umpire_roster=include_umpire_roster,
        )
    except Exception as exc:
        logger.exception(
            "MLB context ingest failed for season=%s start=%s end=%s",
            season,
            start_date,
            end_date,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/season/statcast-batters/load")
async def load_mlb_statcast_batters(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    minimum: str = Query("0", description="Savant minimum threshold; use 0 for exhaustive"),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_savant_statcast_batters(db, season=season, minimum=minimum)
    except Exception as exc:
        logger.exception("MLB Savant batter ingest failed for season=%s", season)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/season/statcast-pitchers/load")
async def load_mlb_statcast_pitchers(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    minimum: str = Query("0", description="Savant minimum threshold; use 0 for exhaustive"),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_savant_statcast_pitchers(db, season=season, minimum=minimum)
    except Exception as exc:
        logger.exception("MLB Savant pitcher ingest failed for season=%s", season)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/season/bat-tracking/load")
async def load_mlb_bat_tracking(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    min_swings: int = Query(0, ge=0),
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
    min_swings: int = Query(0, ge=0),
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


@router.post("/season/savant/load")
async def load_mlb_savant_season_bundle(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    statcast_minimum: str = Query("0", description="Savant minimum threshold; use 0 for exhaustive"),
    bat_tracking_min_swings: int = Query(0, ge=0, description="Bat-tracking and swing-path threshold; use 0 for exhaustive"),
    include_park_factors: bool = Query(True),
    db: AsyncSession = Depends(get_mlb_db),
):
    try:
        return await ingest_savant_season_bundle(
            db,
            season=season,
            statcast_minimum=statcast_minimum,
            bat_tracking_min_swings=bat_tracking_min_swings,
            include_park_factors=include_park_factors,
        )
    except Exception as exc:
        logger.exception("MLB Savant season bundle ingest failed for season=%s", season)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/bootstrap/load")
async def bootstrap_mlb_pipeline(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    since: str = Query(..., description="YYYY-MM-DD"),
    until: str = Query(..., description="YYYY-MM-DD"),
    final_only: bool = Query(False),
    include_savant: bool = Query(True),
    include_weather: bool = Query(False),
    include_umpire_roster: bool = Query(False),
    weather_dataset: str = Query("auto", description="auto, forecast, or historical_forecast"),
    statcast_minimum: str = Query("0", description="Savant minimum threshold; use 0 for exhaustive"),
    bat_tracking_min_swings: int = Query(0, ge=0, description="Bat-tracking and swing-path threshold; use 0 for exhaustive"),
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
            include_weather=include_weather,
            include_umpire_roster=include_umpire_roster,
            weather_dataset=weather_dataset,
            statcast_minimum=statcast_minimum,
            bat_tracking_min_swings=bat_tracking_min_swings,
        )
    except Exception as exc:
        logger.exception(
            "MLB bootstrap failed for season=%s since=%s until=%s",
            season,
            since,
            until,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/bootstrap/ingest/start")
@router.post("/bootstrap/load/start")
async def start_mlb_bootstrap_ingest(
    season: int = Query(..., description="MLB season year, e.g. 2026"),
    since: str = Query(..., description="YYYY-MM-DD"),
    until: str = Query(..., description="YYYY-MM-DD"),
    final_only: bool = Query(False),
    include_savant: bool = Query(False, description="Set true for heavier Savant season refreshes."),
    include_weather: bool = Query(True, description="Load weather snapshots for the schedule window."),
    include_umpire_roster: bool = Query(True),
    weather_dataset: str = Query("auto", description="auto, forecast, or historical_forecast"),
    statcast_minimum: str = Query("0", description="Savant minimum threshold; use 0 for exhaustive"),
    bat_tracking_min_swings: int = Query(0, ge=0, description="Bat-tracking and swing-path threshold; use 0 for exhaustive"),
):
    since = _validate_yyyy_mm_dd(since, "since")
    until = _validate_yyyy_mm_dd(until, "until")
    if datetime.strptime(since, "%Y-%m-%d").date() > datetime.strptime(until, "%Y-%m-%d").date():
        raise HTTPException(status_code=400, detail="since must be on or before until")

    job_id = f"mlb-ingest-{uuid.uuid4()}"
    mlb_ingest_jobs[job_id] = {
        "job_id": job_id,
        "type": "mlb_bootstrap_ingest",
        "status": "queued",
        "season": season,
        "since": since,
        "until": until,
        "final_only": final_only,
        "include_savant": include_savant,
        "include_weather": include_weather,
        "include_umpire_roster": include_umpire_roster,
        "weather_dataset": weather_dataset,
        "statcast_minimum": statcast_minimum,
        "bat_tracking_min_swings": bat_tracking_min_swings,
        "created_at": datetime.utcnow().isoformat(),
        "started_at": None,
        "finished_at": None,
        "result": None,
        "error": None,
    }
    asyncio.create_task(
        _run_mlb_bootstrap_ingest_job(
            job_id,
            season=season,
            since=since,
            until=until,
            final_only=final_only,
            include_savant=include_savant,
            include_weather=include_weather,
            include_umpire_roster=include_umpire_roster,
            weather_dataset=weather_dataset,
            statcast_minimum=statcast_minimum,
            bat_tracking_min_swings=bat_tracking_min_swings,
        )
    )
    return {"status": "queued", "job_id": job_id}


@router.post("/nightly/ingest/start")
@router.post("/nightly/load/start")
async def start_mlb_nightly_ingest(
    season: int | None = Query(None, description="Defaults to current MLB date year."),
    days_back: int = Query(1, ge=0, le=30),
    days_forward: int = Query(1, ge=0, le=14),
    final_only: bool = Query(False),
    include_savant: bool = Query(False, description="Keep false for the nightly lightweight job."),
    include_weather: bool = Query(True),
    include_umpire_roster: bool = Query(True),
    weather_dataset: str = Query("auto", description="auto, forecast, or historical_forecast"),
    statcast_minimum: str = Query("0", description="Savant minimum threshold; use 0 for exhaustive"),
    bat_tracking_min_swings: int = Query(0, ge=0),
):
    target_date = _current_mlb_date()
    since = (target_date - timedelta(days=days_back)).isoformat()
    until = (target_date + timedelta(days=days_forward)).isoformat()
    return await start_mlb_bootstrap_ingest(
        season=season or target_date.year,
        since=since,
        until=until,
        final_only=final_only,
        include_savant=include_savant,
        include_weather=include_weather,
        include_umpire_roster=include_umpire_roster,
        weather_dataset=weather_dataset,
        statcast_minimum=statcast_minimum,
        bat_tracking_min_swings=bat_tracking_min_swings,
    )


@router.get("/ingest/jobs/{job_id}")
async def get_mlb_ingest_job(job_id: str):
    job = mlb_ingest_jobs.get(job_id)
    if not job:
        return {"status": "not_found", "job_id": job_id}
    return job


@router.get("/cron/status")
async def get_mlb_cron_status(
    date: str | None = Query(None, description="YYYY-MM-DD. Defaults to current MLB date."),
    db: AsyncSession = Depends(get_mlb_db),
):
    target_date_value = _parse_yyyy_mm_dd_date(date or _current_mlb_date().isoformat(), "date")

    stmt = text(
        """
        WITH target_games AS (
            SELECT game_pk
            FROM mlb_games
            WHERE official_date = :target_date
        ),
        prediction_counts AS (
            SELECT
                market,
                count(*)::integer AS rows,
                max(updated_at) AS latest_updated_at,
                max(model_path) AS model_path
            FROM mlb_prediction_logs
            WHERE game_date = :target_date
            GROUP BY market
        )
        SELECT
            (SELECT count(*)::integer FROM target_games) AS games_count,
            (
                SELECT max(last_ingested_at)
                FROM mlb_games
                WHERE official_date = :target_date
            ) AS latest_game_ingested_at,
            (
                SELECT count(*)::integer
                FROM mlb_roster_snapshots
                WHERE roster_date = :target_date
            ) AS roster_rows,
            (
                SELECT max(captured_at)
                FROM mlb_roster_snapshots
                WHERE roster_date = :target_date
            ) AS latest_roster_captured_at,
            (
                SELECT count(*)::integer
                FROM mlb_weather_snapshots ws
                JOIN target_games tg ON tg.game_pk = ws.game_pk
            ) AS weather_rows,
            (
                SELECT max(pulled_at)
                FROM mlb_weather_snapshots ws
                JOIN target_games tg ON tg.game_pk = ws.game_pk
            ) AS latest_weather_pulled_at,
            (
                SELECT count(*)::integer
                FROM mlb_game_official_assignments oa
                JOIN target_games tg ON tg.game_pk = oa.game_pk
            ) AS official_assignment_rows,
            (
                SELECT count(*)::integer
                FROM mlb_prop_odds_snapshots
                WHERE game_date = :target_date
                  AND market = 'batter_home_runs'
            ) AS hr_odds_rows,
            (
                SELECT max(fetched_at)
                FROM mlb_prop_odds_snapshots
                WHERE game_date = :target_date
                  AND market = 'batter_home_runs'
            ) AS latest_hr_odds_fetched_at,
            (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'market', market,
                        'rows', rows,
                        'latest_updated_at', latest_updated_at,
                        'model_path', model_path
                    )
                    ORDER BY market
                )
                FROM prediction_counts
            ) AS prediction_markets,
            (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'provider', provider,
                        'bookmaker', bookmaker,
                        'status', status,
                        'props_count', props_count,
                        'events_count', events_count,
                        'fetched_at', fetched_at
                    )
                    ORDER BY fetched_at DESC
                )
                FROM mlb_prop_odds_fetch_logs
                WHERE game_date = :target_date
            ) AS odds_fetch_logs
        """
    )
    row = (await db.execute(stmt, {"target_date": target_date_value})).mappings().first()
    payload = dict(row or {})
    prediction_markets = payload.get("prediction_markets") or []
    market_counts = {
        item["market"]: item["rows"]
        for item in prediction_markets
        if isinstance(item, dict) and item.get("market")
    }
    expected_prediction_markets = [
        "batter_home_runs",
        "batter_hits",
        "batter_total_bases",
        "pitcher_strikeouts",
    ]
    return {
        "status": "ok",
        "date": target_date_value.isoformat(),
        **payload,
        "prediction_market_counts": market_counts,
        "predictions_complete": all(market_counts.get(market, 0) > 0 for market in expected_prediction_markets),
        "ingestion_has_schedule": int(payload.get("games_count") or 0) > 0,
        "ingestion_has_rosters": int(payload.get("roster_rows") or 0) > 0,
        "ingestion_has_weather": int(payload.get("weather_rows") or 0) > 0,
    }
