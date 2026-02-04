from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db, AsyncSessionLocal
from app.services.theodds_client import TheOddsClient
from app.services.nba_client import NBAClient
from app.db.store_odds import save_event_odds
from app.db.store_player_game_stats import save_last_n_games
from app.db.store_team_game_stats import save_team_game_stats
from app.db.store_lineup_stats import save_lineup_stats
from app.db.store_teams import load_teams
from app.db.store_schedule import load_schedule
from app.db.under_risk import compute_under_risk
from nba_api.stats.static import players
from nba_api.stats.static import teams as nba_teams
from sqlalchemy import select, func
from datetime import datetime
from app.models.player_game_stat import PlayerGameStat
from app.models.team_game_stat import TeamGameStat
from app.models.player import Player
from app.models.game_schedule import GameSchedule
import logging
import asyncio
import httpx
import uuid
from asyncio_throttle import Throttler
import pandas as pd
from app.core.constants import MAX_GAMES_PER_PLAYER
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

router = APIRouter()
odds_client = TheOddsClient()
nba_client = NBAClient()

#  throttler to prevent hammering NBA API. rate limiting to 5 requests/sec
throttler = Throttler(rate_limit=5, period=1)
update_jobs: dict[str, dict] = {}


# store player points props for a single event (game)
@router.post("/player-points/{event_id}")
async def refresh_player_points_event(
    event_id: str,
    markets: str = "player_points",
    regions: str | None = None,
    bookmakers: str | None = "sportsbet",
    min_remaining_after_call: int = 3,
    db: AsyncSession = Depends(get_db),
):
    try:
        response = await odds_client.get_event_odds(
            sport="basketball_nba",
            event_id=event_id,
            markets=markets,
            regions=regions,
            bookmakers=bookmakers,
            min_remaining_after_call=min_remaining_after_call,
        )
        odds_data = response["data"]
        await save_event_odds(odds_data, db)
        return {
            "status": "success",
            "event_id": event_id,
            "bookmakers_seen": len(odds_data.get("bookmakers", [])),
            "usage": response.get("usage"),
            "estimated_cost": response.get("estimated_cost"),
        }
    except Exception as e:
        logger.error(f"Error fetching/storing event {event_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# One-call sync for all upcoming events.
# This is much cheaper than looping per-event odds calls.
@router.post("/player-points/all")
async def refresh_all_player_points(
    markets: str = (
        "player_points,player_assists,player_rebounds,"
        "player_points_rebounds_assists,player_points_rebounds,"
        "player_points_assists,player_rebounds_assists"
    ),
    regions: str | None = None,
    bookmakers: str | None = "sportsbet",
    min_remaining_after_call: int = 3,
    max_events: int = 4,
    schedule_mode: str = "auto",
    event_ids: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    try:
        events = await odds_client.get_events("basketball_nba")
        events = sorted(events, key=lambda e: e.get("commence_time", ""))

        now_utc = datetime.now(ZoneInfo("UTC"))
        now_au = datetime.now(ZoneInfo("Australia/Sydney"))
        au_hour = now_au.hour
        mode = schedule_mode.lower().strip()

        if mode == "auto":
            if 18 <= au_hour <= 23:
                mode = "night"
            elif 7 <= au_hour <= 11:
                mode = "morning"
            else:
                mode = "skip"

        if mode == "night":
            min_hours_to_tipoff = 10
            max_hours_to_tipoff = 30
        elif mode == "morning":
            min_hours_to_tipoff = 0
            max_hours_to_tipoff = 8
        elif mode == "all":
            min_hours_to_tipoff = 0
            max_hours_to_tipoff = 36
        elif mode == "skip":
            min_hours_to_tipoff = 9999
            max_hours_to_tipoff = 9999
        else:
            raise HTTPException(
                status_code=400,
                detail="schedule_mode must be one of: auto, night, morning, all",
            )

        selected_ids = set()
        if event_ids:
            selected_ids = {eid.strip() for eid in event_ids.split(",") if eid.strip()}

        filtered_events = []
        if selected_ids:
            for event in events:
                if event.get("id") in selected_ids:
                    filtered_events.append(event)
        else:
            for event in events:
                commence_time = event.get("commence_time")
                if not commence_time:
                    continue
                event_time = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
                hours_to_tipoff = (event_time - now_utc).total_seconds() / 3600
                if min_hours_to_tipoff <= hours_to_tipoff <= max_hours_to_tipoff:
                    filtered_events.append(event)

            if max_events > 0:
                filtered_events = filtered_events[:max_events]

        events_processed = 0
        events_skipped = 0
        total_bookmakers = 0
        total_estimated_cost = 0
        last_usage = odds_client.latest_usage()

        for event in filtered_events:
            event_id = event.get("id")
            if not event_id:
                events_skipped += 1
                continue
            try:
                response = await odds_client.get_event_odds(
                    sport="basketball_nba",
                    event_id=event_id,
                    markets=markets,
                    regions=regions,
                    bookmakers=bookmakers,
                    min_remaining_after_call=min_remaining_after_call,
                )
            except RuntimeError as quota_error:
                logger.warning(f"Stopping prop sync due to quota guard: {quota_error}")
                break

            odds_data = response["data"]
            await save_event_odds(odds_data, db)
            events_processed += 1
            total_bookmakers += len(odds_data.get("bookmakers", []))
            total_estimated_cost += int(response.get("estimated_cost") or 0)
            last_usage = response.get("usage") or last_usage

        return {
            "status": "success",
            "events_processed": events_processed,
            "events_skipped": events_skipped,
            "events_considered": len(filtered_events),
            "events_available": len(events),
            "bookmakers_seen": total_bookmakers,
            "usage": last_usage,
            "estimated_cost": total_estimated_cost,
            "markets_requested": markets,
            "bookmakers_requested": bookmakers,
            "max_events": max_events,
            "schedule_mode": mode,
            "selected_event_count": len(selected_ids),
            "timezone": "Australia/Sydney",
            "sync_local_hour": au_hour,
        }
    except Exception as e:
        logger.error(f"Error fetching/storing all events: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/player-props/sync")
async def refresh_player_props_sync(
    markets: str = (
        "player_points,player_assists,player_rebounds,"
        "player_points_rebounds_assists,player_points_rebounds,"
        "player_points_assists,player_rebounds_assists"
    ),
    regions: str | None = None,
    bookmakers: str | None = "sportsbet",
    min_remaining_after_call: int = 3,
    max_events: int = 4,
    schedule_mode: str = "auto",
    event_ids: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    return await refresh_all_player_points(
        markets=markets,
        regions=regions,
        bookmakers=bookmakers,
        min_remaining_after_call=min_remaining_after_call,
        max_events=max_events,
        schedule_mode=schedule_mode,
        event_ids=event_ids,
        db=db,
    )


# get and store last N box score stats for ALL active players. N takes on the value of MAX_GAMES_PER_PLAYER
# MIGHT TAKE A LONG TIME
@router.post("/last-n/all")
async def store_last_n_games_all_players(
    season: str = "2025-26", db: AsyncSession = Depends(get_db)
):
    active_players = players.get_active_players()
    saved = 0
    skipped = 0
    failed = 0
    total_new_games = 0

    for p in active_players:
        player_id = p["id"]
        player_name = p["full_name"]

        # Retry logic to try up to 3 times if fetch fails
        for attempt in range(3):
            try:
                # rate limiting to max 5 requests per second
                async with throttler:
                    df = nba_client.fetch_player_game_log(player_id, season)
                break
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {player_name}: {e}")
                await asyncio.sleep(0.5)
        else:
            logger.error(f"All retries failed for {player_name}")
            failed += 1
            continue

        if df.empty:
            skipped += 1
            continue

        team_abbr = (
            df.iloc[0]["TEAM_ABBREVIATION"]
            if "TEAM_ABBREVIATION" in df.columns
            else None
        )
        if not team_abbr:
            for attempt in range(2):
                try:
                    async with throttler:
                        info_df, _ = nba_client.fetch_player_info(player_id)
                    if not info_df.empty and "TEAM_ABBREVIATION" in info_df.columns:
                        team_abbr = info_df.iloc[0]["TEAM_ABBREVIATION"]
                    break
                except Exception as e:
                    logger.warning(
                        f"Attempt {attempt + 1} failed for player info {player_name}: {e}"
                    )
                    await asyncio.sleep(0.5)
        if not team_abbr:
            for attempt in range(2):
                try:
                    async with throttler:
                        info_df, _ = nba_client.fetch_player_info(player_id)
                    if not info_df.empty and "TEAM_ABBREVIATION" in info_df.columns:
                        team_abbr = info_df.iloc[0]["TEAM_ABBREVIATION"]
                    break
                except Exception as e:
                    logger.warning(
                        f"Attempt {attempt + 1} failed for player info {player_name}: {e}"
                    )
                    await asyncio.sleep(0.5)

        try:
            new_games = await save_last_n_games(
                player_id=player_id,
                player_name=player_name,
                team_abbr=team_abbr,
                df=df,
                db=db,
            )

            if new_games > 0:
                saved += 1
                total_new_games += new_games
            else:
                skipped += 1

        except Exception as e:
            logger.warning(f"Failed saving {player_name}: {e}")
            failed += 1
            continue

        # small delay for safety
        await asyncio.sleep(0.05)

    return {
        "status": "completed",
        "players_total": len(active_players),
        "players_saved": saved,
        "players_skipped": skipped,
        "players_failed": failed,
        "total_new_games_inserted": total_new_games,
    }


async def _run_last_n_update(
    since_date,
    season: str,
    db: AsyncSession,
    job_id: str | None = None,
):
    today = datetime.utcnow().date()
    schedule_result = await db.execute(
        select(GameSchedule.game_id, GameSchedule.game_date).where(
            GameSchedule.game_date > since_date,
            GameSchedule.game_date <= today,
        )
    )
    game_rows = schedule_result.all()
    player_map = {}

    if game_rows:
        for game_id, _game_date in game_rows:
            if not game_id:
                continue
            for attempt in range(3):
                try:
                    async with throttler:
                        game_players = nba_client.fetch_game_players(game_id)
                    break
                except Exception as e:
                    logger.warning(
                        f"Attempt {attempt + 1} failed for game {game_id}: {e}"
                    )
                    await asyncio.sleep(0.5)
            else:
                logger.error(f"All retries failed for game {game_id}")
                continue

            for p in game_players:
                pid = p.get("PLAYER_ID")
                name = p.get("PLAYER_NAME")
                if pid:
                    player_map[int(pid)] = name or player_map.get(int(pid)) or str(pid)

    if player_map:
        active_players = [
            {"id": pid, "full_name": name} for pid, name in player_map.items()
        ]
        logger.info(
            f"update_last_n_games_since: {len(game_rows)} games, "
            f"{len(active_players)} players since {since_date}"
        )
    else:
        logger.warning(
            "update_last_n_games_since: no recent games found in schedule; "
            "falling back to active players list"
        )
        active_players = players.get_active_players()
    saved = 0
    skipped = 0
    failed = 0
    total_new_games = 0

    for idx, p in enumerate(active_players, start=1):
        player_id = p["id"]
        player_name = p["full_name"]

        if job_id and job_id in update_jobs:
            update_jobs[job_id]["players_done"] = idx - 1
            update_jobs[job_id]["players_total"] = len(active_players)

        result = await db.execute(
            select(func.max(PlayerGameStat.game_date)).where(
                PlayerGameStat.player_id == player_id
            )
        )
        last_game_date = result.scalar_one_or_none()
        if last_game_date and last_game_date >= since_date:
            skipped += 1
            continue

        for attempt in range(3):
            try:
                async with throttler:
                    df = nba_client.fetch_player_game_log(player_id, season)
                break
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {player_name}: {e}")
                await asyncio.sleep(0.5)
        else:
            logger.error(f"All retries failed for {player_name}")
            failed += 1
            continue

        if df.empty:
            skipped += 1
            continue

        team_abbr = (
            df.iloc[0]["TEAM_ABBREVIATION"]
            if "TEAM_ABBREVIATION" in df.columns
            else None
        )
        if not team_abbr:
            for attempt in range(2):
                try:
                    async with throttler:
                        info_df, _ = nba_client.fetch_player_info(player_id)
                    if not info_df.empty and "TEAM_ABBREVIATION" in info_df.columns:
                        team_abbr = info_df.iloc[0]["TEAM_ABBREVIATION"]
                    break
                except Exception as e:
                    logger.warning(
                        f"Attempt {attempt + 1} failed for player info {player_name}: {e}"
                    )
                    await asyncio.sleep(0.5)

        try:
            new_games = await save_last_n_games(
                player_id=player_id,
                player_name=player_name,
                team_abbr=team_abbr,
                df=df,
                db=db,
            )

            if new_games > 0:
                saved += 1
                total_new_games += new_games
            else:
                skipped += 1

        except Exception as e:
            logger.warning(f"Failed saving {player_name}: {e}")
            failed += 1
            continue

        await asyncio.sleep(0.05)

    return {
        "status": "completed",
        "players_total": len(active_players),
        "players_saved": saved,
        "players_skipped": skipped,
        "players_failed": failed,
        "total_new_games_inserted": total_new_games,
        "since": str(since_date),
    }


@router.post("/last-n/update")
async def update_last_n_games_since(
    since: str,
    season: str = "2025-26",
    db: AsyncSession = Depends(get_db),
):
    """
    Update last-n games only for players whose latest stored game is older than `since`.
    `since` should be YYYY-MM-DD.
    """
    try:
        since_date = datetime.strptime(since, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="since must be YYYY-MM-DD")
    return await _run_last_n_update(since_date, season, db)


async def _run_last_n_update_job(job_id: str, since: str, season: str):
    try:
        since_date = datetime.strptime(since, "%Y-%m-%d").date()
        update_jobs[job_id]["status"] = "running"
        async with AsyncSessionLocal() as db:
            result = await _run_last_n_update(since_date, season, db, job_id=job_id)
        update_jobs[job_id]["status"] = "completed"
        update_jobs[job_id]["result"] = result
        update_jobs[job_id]["finished_at"] = datetime.utcnow().isoformat()
    except Exception as e:
        logger.error(f"last-n update job {job_id} failed: {e}")
        update_jobs[job_id]["status"] = "failed"
        update_jobs[job_id]["error"] = str(e)
        update_jobs[job_id]["finished_at"] = datetime.utcnow().isoformat()


@router.post("/last-n/update/start")
async def start_update_last_n_games_since(
    since: str,
    season: str = "2025-26",
):
    """
    Start last-n update in the background and return a job id for polling.
    """
    try:
        datetime.strptime(since, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="since must be YYYY-MM-DD")

    job_id = str(uuid.uuid4())
    update_jobs[job_id] = {
        "job_id": job_id,
        "type": "last_n_update",
        "status": "queued",
        "since": since,
        "season": season,
        "players_done": 0,
        "players_total": None,
        "created_at": datetime.utcnow().isoformat(),
        "result": None,
        "error": None,
    }
    asyncio.create_task(_run_last_n_update_job(job_id, since, season))
    return {"status": "queued", "job_id": job_id}


@router.get("/jobs/{job_id}")
async def get_update_job_status(job_id: str):
    job = update_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@router.post("/last-n/backfill-shooting")
async def backfill_shooting_stats(
    season: str = "2025-26",
    db: AsyncSession = Depends(get_db),
    limit: int | None = None,
):
    """
    One-off backfill for players whose player_game_stats rows are missing shooting fields.
    Only refetches those players and updates missing columns (no full reinsert).
    """
    result = await db.execute(
        select(PlayerGameStat.player_id)
        .where(
            (PlayerGameStat.fg3m.is_(None))
            | (PlayerGameStat.fg3a.is_(None))
            | (PlayerGameStat.fgm.is_(None))
            | (PlayerGameStat.fga.is_(None))
        )
        .distinct()
    )
    player_ids = [row[0] for row in result.all()]
    if limit:
        player_ids = player_ids[:limit]

    saved = 0
    skipped = 0
    failed = 0
    total_updates = 0

    for player_id in player_ids:
        player = await db.get(Player, player_id)
        player_name = player.full_name if player else str(player_id)
        team_abbr = player.team_abbreviation if player else None

        for attempt in range(3):
            try:
                async with throttler:
                    df = nba_client.fetch_player_game_log(player_id, season)
                break
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {player_name}: {e}")
                await asyncio.sleep(0.5)
        else:
            logger.error(f"All retries failed for {player_name}")
            failed += 1
            continue

        if df.empty:
            skipped += 1
            continue

        try:
            updates = await save_last_n_games(
                player_id=player_id,
                player_name=player_name,
                team_abbr=team_abbr,
                df=df,
                db=db,
            )
            if updates > 0:
                saved += 1
                total_updates += updates
            else:
                skipped += 1
        except Exception as e:
            logger.warning(f"Failed backfill for {player_name}: {e}")
            failed += 1
            continue

        await asyncio.sleep(0.05)

    return {
        "status": "completed",
        "players_targeted": len(player_ids),
        "players_saved": saved,
        "players_skipped": skipped,
        "players_failed": failed,
        "total_rows_updated": total_updates,
    }


@router.post("/players/backfill-team-abbr")
async def backfill_player_team_abbr(
    db: AsyncSession = Depends(get_db),
    limit: int | None = None,
):
    """
    Backfill missing player team_abbreviation using NBA API player info.
    """
    result = await db.execute(select(Player).where(Player.team_abbreviation.is_(None)))
    players_missing = result.scalars().all()
    if limit:
        players_missing = players_missing[:limit]

    updated = 0
    failed = 0

    for p in players_missing:
        player_id = p.id
        player_name = p.full_name
        for attempt in range(3):
            try:
                async with throttler:
                    info_df, _ = nba_client.fetch_player_info(player_id)
                if not info_df.empty and "TEAM_ABBREVIATION" in info_df.columns:
                    team_abbr = info_df.iloc[0]["TEAM_ABBREVIATION"]
                    if team_abbr:
                        p.team_abbreviation = team_abbr
                        updated += 1
                break
            except Exception as e:
                logger.warning(
                    f"Attempt {attempt + 1} failed for player info {player_name}: {e}"
                )
                await asyncio.sleep(0.5)
        else:
            failed += 1

        await asyncio.sleep(0.05)

    await db.commit()

    return {
        "status": "completed",
        "players_targeted": len(players_missing),
        "players_updated": updated,
        "players_failed": failed,
    }


@router.post("/team-games/backfill-shooting")
async def backfill_team_shooting_stats(
    season: str = "2025-26",
    db: AsyncSession = Depends(get_db),
    limit: int | None = None,
):
    """
    One-off backfill for teams whose team_game_stats rows are missing shooting fields.
    Only refetches those teams and updates missing columns.
    """
    result = await db.execute(
        select(TeamGameStat.team_id)
        .where(
            (TeamGameStat.fg3m.is_(None))
            | (TeamGameStat.fg3a.is_(None))
            | (TeamGameStat.fgm.is_(None))
            | (TeamGameStat.fga.is_(None))
        )
        .distinct()
    )
    team_ids = [row[0] for row in result.all()]
    if limit:
        team_ids = team_ids[:limit]

    saved = 0
    skipped = 0
    failed = 0
    total_updates = 0

    teams_list = nba_teams.get_teams()
    team_meta = {t["id"]: t for t in teams_list}

    for team_id in team_ids:
        team = team_meta.get(team_id, {})
        team_name = team.get("full_name", str(team_id))
        team_abbr = team.get("abbreviation")

        for attempt in range(3):
            try:
                async with throttler:
                    df = nba_client.fetch_team_game_log(team_id, season)
                break
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {team_name}: {e}")
                await asyncio.sleep(0.5)
        else:
            logger.error(f"All retries failed for {team_name}")
            failed += 1
            continue

        if df.empty:
            skipped += 1
            continue

        try:
            updates = await save_team_game_stats(
                team_id=team_id,
                df=df,
                db=db,
                team_abbr=team_abbr,
            )
            if updates > 0:
                saved += 1
                total_updates += updates
            else:
                skipped += 1
        except Exception as e:
            logger.warning(f"Failed backfill for {team_name}: {e}")
            failed += 1
            continue

        await asyncio.sleep(0.05)

    return {
        "status": "completed",
        "teams_targeted": len(team_ids),
        "teams_saved": saved,
        "teams_skipped": skipped,
        "teams_failed": failed,
        "total_rows_updated": total_updates,
    }


@router.post("/team-games/all")
async def store_team_games_all_teams(
    season: str = "2025-26",
    db: AsyncSession = Depends(get_db),
):
    teams = nba_teams.get_teams()
    inserted = 0
    skipped = 0
    failed = 0

    for t in teams:
        team_id = t["id"]
        team_name = t["full_name"]
        team_abbr = t.get("abbreviation")

        for attempt in range(3):
            try:
                async with throttler:
                    df = nba_client.fetch_team_game_log(team_id, season)
                break
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {team_name}: {e}")
                await asyncio.sleep(0.5)
        else:
            logger.error(f"All retries failed for {team_name}")
            failed += 1
            continue

        if df.empty:
            skipped += 1
            continue

        try:
            new_games = await save_team_game_stats(
                team_id=team_id,
                df=df,
                db=db,
                team_abbr=team_abbr,
            )
            if new_games > 0:
                inserted += new_games
            else:
                skipped += 1
        except Exception as e:
            logger.warning(f"Failed saving team games for {team_name}: {e}")
            failed += 1
            continue

        await asyncio.sleep(0.05)

    return {
        "status": "completed",
        "teams_total": len(teams),
        "rows_inserted": inserted,
        "teams_skipped": skipped,
        "teams_failed": failed,
    }


@router.post("/team-games/update")
async def update_team_games_all_teams(
    season: str = "2025-26",
    db: AsyncSession = Depends(get_db),
):
    teams = nba_teams.get_teams()
    inserted = 0
    skipped = 0
    failed = 0

    for t in teams:
        team_id = t["id"]
        team_name = t["full_name"]
        team_abbr = t.get("abbreviation")

        result = await db.execute(
            select(func.max(TeamGameStat.game_date)).where(
                TeamGameStat.team_id == team_id
            )
        )
        last_game_date = result.scalar_one_or_none()

        for attempt in range(3):
            try:
                async with throttler:
                    df = nba_client.fetch_team_game_log(team_id, season)
                break
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {team_name}: {e}")
                await asyncio.sleep(0.5)
        else:
            logger.error(f"All retries failed for {team_name}")
            failed += 1
            continue

        if df.empty:
            skipped += 1
            continue

        try:
            new_games = await save_team_game_stats(
                team_id=team_id,
                df=df,
                db=db,
                team_abbr=team_abbr,
                last_game_date=last_game_date,
            )
            if new_games > 0:
                inserted += new_games
            else:
                skipped += 1
        except Exception as e:
            logger.warning(f"Failed saving team games for {team_name}: {e}")
            failed += 1
            continue

        await asyncio.sleep(0.05)

    return {
        "status": "completed",
        "teams_total": len(teams),
        "rows_inserted": inserted,
        "teams_skipped": skipped,
        "teams_failed": failed,
    }


@router.post("/lineups/all")
async def store_lineups_all_teams(
    season: str = "2025-26",
    group_quantity: int = 5,
    db: AsyncSession = Depends(get_db),
):
    teams = nba_teams.get_teams()
    inserted = 0
    skipped = 0
    failed = 0

    for t in teams:
        team_id = t["id"]
        team_name = t["full_name"]

        for attempt in range(3):
            try:
                async with throttler:
                    df = nba_client.fetch_team_lineups(
                        team_id, season=season, group_quantity=group_quantity
                    )
                break
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {team_name}: {e}")
                await asyncio.sleep(0.5)
        else:
            logger.error(f"All retries failed for {team_name}")
            failed += 1
            continue

        if df.empty:
            skipped += 1
            continue

        try:
            new_rows = await save_lineup_stats(
                team_id=team_id,
                season=season,
                df=df,
                db=db,
            )
            if new_rows > 0:
                inserted += new_rows
            else:
                skipped += 1
        except Exception as e:
            logger.warning(f"Failed saving lineups for {team_name}: {e}")
            failed += 1
            continue

        await asyncio.sleep(0.05)

    return {
        "status": "completed",
        "teams_total": len(teams),
        "rows_inserted": inserted,
        "teams_skipped": skipped,
        "teams_failed": failed,
    }


# get and store the last N box score stats for the givn player. N takes on the value of MAX_GAMES_PER_PLAYER
@router.post("/last-n/{player_id}")
async def store_last_n_games_player(
    player_id: int,
    season: str = "2025-26",
    db: AsyncSession = Depends(get_db),
):
    df = nba_client.fetch_player_game_log(player_id, season)

    if df.empty:
        return {"status": "no data"}

    info_df, _ = nba_client.fetch_player_info(player_id)

    player_name = info_df.iloc[0]["DISPLAY_FIRST_LAST"]
    team_abbr = info_df.iloc[0]["TEAM_ABBREVIATION"]

    new_games = await save_last_n_games(
        player_id=player_id,
        player_name=player_name,
        team_abbr=team_abbr,
        df=df,
        db=db,
    )

    return {
        "status": "saved",
        "games": new_games,
    }


# Get and store the team metadata
@router.post("/teams/load")
async def load_all_teams(db: AsyncSession = Depends(get_db)):
    """
    Load all NBA teams from nba_api static endpoints into DB.
    """
    try:
        await load_teams(db)
        return {"status": "success", "message": "Teams loaded/updated"}
    except Exception as e:
        logger.error(f"Error loading teams: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# fetch and store the schedule info for the given season
@router.post("/schedule/load")
async def load_season_schedule(
    season: str = "2025-26",
    db: AsyncSession = Depends(get_db),
):
    """
    Load all games for a season from NBA API into DB.
    """
    try:
        await load_schedule(db, season)
        return {
            "status": "success",
            "season": season,
            "message": "Schedule loaded/updated",
        }
    except Exception as e:
        logger.error(f"Error loading schedule for season {season}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/under-risk/recalc")
async def recalc_under_risk(
    stat_type: str,
    window_n: int = 20,
    db: AsyncSession = Depends(get_db),
):
    """
    Recalculate under-risk for a stat_type using prediction_logs + actuals.
    """
    try:
        return await compute_under_risk(db, stat_type, window_n)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error computing under-risk for {stat_type}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/under-risk/recalc-all")
async def recalc_under_risk_all(
    window_n: int = 20,
    db: AsyncSession = Depends(get_db),
):
    """
    Recalculate under-risk for all supported stat types.
    """
    stat_types = ["points", "assists", "rebounds", "threept"]
    results = {}

    for stat_type in stat_types:
        try:
            results[stat_type] = await compute_under_risk(db, stat_type, window_n)
        except Exception as e:
            logger.error(f"Error computing under-risk for {stat_type}: {e}")
            results[stat_type] = {"status": "error", "detail": str(e)}

    return {
        "status": "completed",
        "window_n": window_n,
        "results": results,
    }
