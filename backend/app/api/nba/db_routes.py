from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import create_engine
from app.db.session import get_db, AsyncSessionLocal
from app.core.config import settings
from app.db.url_utils import to_sync_db_url
from app.services.theodds_client import TheOddsClient
from app.services.nba_client import NBAClient
from app.db.nba.store_odds import save_event_odds
from app.db.nba.store_player_game_stats import save_last_n_games
from app.db.nba.store_team_game_stats import save_team_game_stats
from app.db.nba.store_lineup_stats import save_lineup_stats
from app.db.nba.store_teams import load_teams
from app.db.nba.store_schedule import load_schedule
from app.db.nba.under_risk import compute_under_risk
from app.db.nba.store_prediction_logs import update_prediction_actuals, log_predictions
from nba_api.stats.static import players
from nba_api.stats.static import teams as nba_teams
from sqlalchemy import select, func, text
from datetime import datetime, timedelta
from app.models.player_game_stat import PlayerGameStat
from app.models.team_game_stat import TeamGameStat
from app.models.player import Player
from app.models.game_schedule import GameSchedule
from app.models.ingestion_run import IngestionRun
import logging
import asyncio
import httpx
import uuid
import random
from asyncio_throttle import Throttler
import pandas as pd
from app.core.constants import MAX_GAMES_PER_PLAYER
from zoneinfo import ZoneInfo
from ml.nba.backtest import walk_forward_backtest
from ml.nba.update_rolling import update_rolling_stats


def _parse_minutes(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if ":" in text:
            parts = text.split(":")
            if len(parts) == 2:
                try:
                    minutes = float(parts[0])
                    seconds = float(parts[1])
                    return minutes + (seconds / 60.0)
                except ValueError:
                    return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _normalize_text(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    return text or None


logger = logging.getLogger(__name__)

router = APIRouter()
odds_client = TheOddsClient()
nba_client = NBAClient()
# Dedicated client for per-game boxscore ingest.
nba_boxscore_client = NBAClient(timeout=60)
sync_engine = create_engine(to_sync_db_url(settings.DATABASE_URL))

# Throttle NBA API calls conservatively to reduce upstream timeouts.
throttler = Throttler(rate_limit=2, period=1)
# Boxscore endpoint is more fragile; keep this much slower.
boxscore_throttler = Throttler(rate_limit=1, period=30)
update_jobs: dict[str, dict] = {}

GAME_INGEST_MAX_ATTEMPTS = 4
GAME_INGEST_BACKOFF_BASE_SECONDS = 1.0
GAME_INGEST_BACKOFF_CAP_SECONDS = 8.0
GAME_INGEST_BACKOFF_JITTER_SECONDS = 0.4


def _current_et_date():
    if ZoneInfo:
        return datetime.now(ZoneInfo("America/New_York")).date()
    return datetime.now().date()


def _latest_completed_nba_et_date():
    # Scheduler should treat the most recent fully completed NBA slate as the
    # previous ET calendar day.
    return _current_et_date() - timedelta(days=1)


def _retry_backoff_seconds(
    attempt_index: int,
    base_seconds: float = GAME_INGEST_BACKOFF_BASE_SECONDS,
    cap_seconds: float = GAME_INGEST_BACKOFF_CAP_SECONDS,
    jitter_seconds: float = GAME_INGEST_BACKOFF_JITTER_SECONDS,
) -> float:
    delay = min(cap_seconds, base_seconds * (2 ** attempt_index))
    return delay + random.uniform(0.0, jitter_seconds)


async def _record_ingest_run(
    db: AsyncSession,
    since_date,
    season: str,
    result: dict,
    status: str,
    note: str | None = None,
    ingest_type: str = "last_n_update",
):
    try:
        run = IngestionRun(
            ingest_type=ingest_type,
            since_date=since_date,
            season=season,
            status=status,
            note=note or result.get("note"),
            players_total=int(result.get("players_total") or 0),
            players_saved=int(result.get("players_saved") or 0),
            players_skipped=int(result.get("players_skipped") or 0),
            players_failed=int(result.get("players_failed") or 0),
            total_new_games_inserted=int(result.get("total_new_games_inserted") or 0),
        )
        db.add(run)
        await db.commit()
    except Exception as e:
        logger.warning(f"Failed to record ingestion run: {e}")


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
        raise HTTPException(
            status_code=500,
            detail="Failed to sync event odds. Please try again later.",
        )


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
        raise HTTPException(
            status_code=500,
            detail="Failed to sync player props window. Please try again later.",
        )


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
    until_date,
    season: str,
    db: AsyncSession,
    job_id: str | None = None,
):
    schedule_result = await db.execute(
        select(
            GameSchedule.game_id,
            GameSchedule.game_date,
            GameSchedule.home_team_abbr,
            GameSchedule.away_team_abbr,
        ).where(
            GameSchedule.game_date >= since_date,
            GameSchedule.game_date <= until_date,
        )
    )
    game_rows = schedule_result.all()
    player_map = {}
    team_abbrs: set[str] = set()

    if not game_rows:
        logger.info(
            "update_last_n_games_since: no scheduled games in range; skipping player sync"
        )
        result = {
            "status": "completed",
            "players_total": 0,
            "players_saved": 0,
            "players_skipped": 0,
            "players_failed": 0,
            "total_new_games_inserted": 0,
            "since": str(since_date),
            "until": str(until_date),
            "season": season,
            "note": "No games found in game_schedule for the requested date range.",
        }
        await _record_ingest_run(db, since_date, season, result, "no_games")
        return result

    if game_rows:
        for game_id, _game_date, home_abbr, away_abbr in game_rows:
            if not game_id:
                continue
            if home_abbr:
                team_abbrs.add(str(home_abbr))
            if away_abbr:
                team_abbrs.add(str(away_abbr))
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
        if team_abbrs:
            logger.warning(
                "update_last_n_games_since: no boxscore players resolved; "
                "falling back to players table by team abbreviations"
            )
            players_result = await db.execute(
                select(Player.id, Player.full_name).where(
                    Player.team_abbreviation.in_(tuple(team_abbrs))
                )
            )
            active_players = [
                {"id": int(pid), "full_name": name}
                for pid, name in players_result.all()
                if pid is not None
            ]
            if not active_players:
                result = {
                    "status": "completed",
                    "players_total": 0,
                    "players_saved": 0,
                    "players_skipped": 0,
                    "players_failed": 0,
                    "total_new_games_inserted": 0,
                    "since": str(since_date),
                    "until": str(until_date),
                    "season": season,
                    "note": "No players found for teams in schedule window.",
                }
                await _record_ingest_run(db, since_date, season, result, "no_players")
                return result
        else:
            logger.warning(
                "update_last_n_games_since: games were found but no team abbreviations resolved"
            )
            result = {
                "status": "completed",
                "players_total": 0,
                "players_saved": 0,
                "players_skipped": 0,
                "players_failed": 0,
                "total_new_games_inserted": 0,
                "since": str(since_date),
                "until": str(until_date),
                "season": season,
                "note": "Game rows exist but no teams/players could be resolved.",
            }
            await _record_ingest_run(db, since_date, season, result, "no_players")
            return result
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

    result = {
        "status": "completed",
        "players_total": len(active_players),
        "players_saved": saved,
        "players_skipped": skipped,
        "players_failed": failed,
        "total_new_games_inserted": total_new_games,
        "since": str(since_date),
        "until": str(until_date),
        "season": season,
    }
    await _record_ingest_run(db, since_date, season, result, "completed")
    return result


async def _run_games_ingest(
    since_date,
    until_date,
    season: str,
    include_team_stats: bool,
    db: AsyncSession,
    job_id: str | None = None,
):
    schedule_result = await db.execute(
        select(
            GameSchedule.game_id,
            GameSchedule.game_date,
            GameSchedule.matchup,
            GameSchedule.home_team_id,
            GameSchedule.away_team_id,
            GameSchedule.home_team_abbr,
            GameSchedule.away_team_abbr,
        ).where(
            GameSchedule.game_date >= since_date,
            GameSchedule.game_date <= until_date,
            GameSchedule.season == season,
        )
    )
    games = schedule_result.all()
    if not games:
        result = {
            "status": "no_games",
            "since": str(since_date),
            "until": str(until_date),
            "season": season,
            "games_considered": 0,
        }
        await _record_ingest_run(
            db,
            since_date,
            season,
            result,
            "no_games",
            note=f"until={until_date}",
            ingest_type="games_ingest",
        )
        return result

    # End the read transaction before long-running external boxscore calls so the
    # session does not hold an idle connection open until the next DB operation.
    await db.rollback()

    if job_id and job_id in update_jobs:
        update_jobs[job_id]["games_total"] = len(games)
        update_jobs[job_id]["games_done"] = 0
        update_jobs[job_id]["current_game_id"] = None
        update_jobs[job_id]["current_game_date"] = None

    games_processed = 0
    games_skipped = 0
    players_inserted = 0
    players_updated = 0
    teams_inserted = 0
    teams_updated = 0

    for idx, game in enumerate(games, start=1):
        (
            game_id,
            game_date,
            matchup,
            home_team_id,
            away_team_id,
            home_abbr,
            away_abbr,
        ) = game
        if not game_id:
            games_skipped += 1
            if job_id and job_id in update_jobs:
                update_jobs[job_id]["games_done"] = idx
            continue

        if job_id and job_id in update_jobs:
            update_jobs[job_id]["games_done"] = idx - 1
            update_jobs[job_id]["current_game_id"] = str(game_id)
            update_jobs[job_id]["current_game_date"] = str(game_date)

        for attempt in range(GAME_INGEST_MAX_ATTEMPTS):
            try:
                async with boxscore_throttler:
                    players_df, teams_df = nba_boxscore_client.fetch_game_boxscore(
                        str(game_id)
                    )
                break
            except Exception as e:
                if attempt + 1 >= GAME_INGEST_MAX_ATTEMPTS:
                    logger.warning(
                        f"Attempt {attempt + 1}/{GAME_INGEST_MAX_ATTEMPTS} failed for game {game_id}: {e}"
                    )
                    continue
                wait_seconds = _retry_backoff_seconds(attempt)
                logger.warning(
                    f"Attempt {attempt + 1}/{GAME_INGEST_MAX_ATTEMPTS} failed for game {game_id}: {e}. "
                    f"Retrying in {wait_seconds:.2f}s"
                )
                await asyncio.sleep(wait_seconds)
        else:
            logger.error(f"All retries failed for game {game_id}")
            games_skipped += 1
            continue

        if players_df is None or players_df.empty:
            games_skipped += 1
            continue

        existing_players_result = await db.execute(
            select(PlayerGameStat).where(PlayerGameStat.game_id == str(game_id))
        )
        existing_players = {}
        for row in existing_players_result.scalars().all():
            try:
                pid = int(row.player_id)
            except (TypeError, ValueError):
                continue
            existing_players[pid] = row

        for _, row in players_df.iterrows():
            player_id = row.get("PLAYER_ID")
            if player_id is None:
                continue
            try:
                player_id = int(player_id)
            except (TypeError, ValueError):
                continue

            player_name = _normalize_text(row.get("PLAYER_NAME"))
            team_abbr = _normalize_text(
                row.get("TEAM_ABBREVIATION") or row.get("TEAM_ABBR")
            )

            player = await db.get(Player, player_id)
            if not player:
                player = Player(
                    id=player_id,
                    full_name=player_name or f"Player {player_id}",
                    team_abbreviation=team_abbr,
                )
                db.add(player)
            elif player_name and not player.full_name:
                player.full_name = player_name
            elif team_abbr and player.team_abbreviation != team_abbr:
                player.team_abbreviation = team_abbr

            existing = existing_players.get(player_id)
            if existing:
                has_updates = False
                for col, key in [
                    ("minutes", "MIN"),
                    ("points", "PTS"),
                    ("assists", "AST"),
                    ("rebounds", "REB"),
                    ("steals", "STL"),
                    ("blocks", "BLK"),
                    ("turnovers", "TOV"),
                    ("fgm", "FGM"),
                    ("fga", "FGA"),
                    ("fg3m", "FG3M"),
                    ("fg3a", "FG3A"),
                ]:
                    value = row.get(key)
                    if col == "minutes":
                        value = _parse_minutes(value)
                    if value is not None and getattr(existing, col) is None:
                        setattr(existing, col, value)
                        has_updates = True
                if has_updates:
                    players_updated += 1
                continue

            db.add(
                PlayerGameStat(
                    player_id=player_id,
                    game_id=str(game_id),
                    game_date=game_date,
                    matchup=matchup,
                    minutes=_parse_minutes(row.get("MIN")),
                    points=row.get("PTS"),
                    assists=row.get("AST"),
                    rebounds=row.get("REB"),
                    steals=row.get("STL"),
                    blocks=row.get("BLK"),
                    turnovers=row.get("TOV"),
                    fgm=row.get("FGM"),
                    fga=row.get("FGA"),
                    fg3m=row.get("FG3M"),
                    fg3a=row.get("FG3A"),
                )
            )
            players_inserted += 1

        if include_team_stats and teams_df is not None and not teams_df.empty:
            teams_df = teams_df.copy()
            if "TEAM_ID" in teams_df.columns:
                teams_df = teams_df.dropna(subset=["TEAM_ID"]).drop_duplicates(
                    subset=["TEAM_ID"], keep="first"
                )
            existing_teams_result = await db.execute(
                select(TeamGameStat).where(TeamGameStat.game_id == str(game_id))
            )
            existing_teams = {
                int(row.team_id): row for row in existing_teams_result.scalars().all()
                if row.team_id is not None
            }

            for _, row in teams_df.iterrows():
                team_id = row.get("TEAM_ID")
                if team_id is None:
                    continue
                try:
                    team_id = int(team_id)
                except (TypeError, ValueError):
                    continue

                team_abbr = _normalize_text(row.get("TEAM_ABBREVIATION"))
                existing_team = existing_teams.get(team_id)
                if existing_team:
                    has_updates = False
                    for col, key in [
                        ("points", "PTS"),
                        ("assists", "AST"),
                        ("rebounds", "REB"),
                        ("turnovers", "TOV"),
                        ("fgm", "FGM"),
                        ("fga", "FGA"),
                        ("fg3m", "FG3M"),
                        ("fg3a", "FG3A"),
                    ]:
                        value = row.get(key)
                        if value is not None and getattr(existing_team, col) is None:
                            setattr(existing_team, col, value)
                            has_updates = True
                    if has_updates:
                        teams_updated += 1
                    continue

                db.add(
                    TeamGameStat(
                        team_id=team_id,
                        team_abbreviation=team_abbr,
                        game_id=str(game_id),
                        game_date=game_date,
                        matchup=matchup,
                        points=row.get("PTS"),
                        assists=row.get("AST"),
                        rebounds=row.get("REB"),
                        turnovers=row.get("TOV"),
                        fgm=row.get("FGM"),
                        fga=row.get("FGA"),
                        fg3m=row.get("FG3M"),
                        fg3a=row.get("FG3A"),
                    )
                )
                teams_inserted += 1

        try:
            await db.commit()
        except Exception as e:
            await db.rollback()
            logger.exception(f"Failed committing game {game_id}: {e}")
            games_skipped += 1
            if job_id and job_id in update_jobs:
                update_jobs[job_id]["games_done"] = idx
            continue
        games_processed += 1
        if job_id and job_id in update_jobs:
            update_jobs[job_id]["games_done"] = idx
        await asyncio.sleep(0.05)

    result = {
        "status": "completed",
        "since": str(since_date),
        "until": str(until_date),
        "season": season,
        "games_considered": len(games),
        "games_processed": games_processed,
        "games_skipped": games_skipped,
        "players_inserted": players_inserted,
        "players_updated": players_updated,
        "teams_inserted": teams_inserted,
        "teams_updated": teams_updated,
        "include_team_stats": include_team_stats,
    }
    if job_id and job_id in update_jobs:
        update_jobs[job_id]["games_done"] = len(games)
        update_jobs[job_id]["current_game_id"] = None
        update_jobs[job_id]["current_game_date"] = None
    await _record_ingest_run(
        db,
        since_date,
        season,
        result,
        "completed",
        note=f"until={until_date}",
        ingest_type="games_ingest",
    )
    return result


@router.post("/games/ingest")
async def ingest_games_by_date(
    since: str,
    until: str | None = None,
    season: str = "2025-26",
    include_team_stats: bool = True,
    db: AsyncSession = Depends(get_db),
):
    """
    Ingest per-game boxscores for all scheduled games in a date range.
    Faster than per-player ingestion for daily runs.
    """
    try:
        since_date = datetime.strptime(since, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="since must be YYYY-MM-DD")

    if until:
        try:
            until_date = datetime.strptime(until, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="until must be YYYY-MM-DD")
    else:
        until_date = _current_et_date()

    if since_date > until_date:
        raise HTTPException(
            status_code=400, detail="since must be on or before until"
        )

    return await _run_games_ingest(
        since_date, until_date, season, include_team_stats, db
    )


async def _run_games_ingest_job(
    job_id: str, since: str, until: str | None, season: str, include_team_stats: bool
):
    try:
        since_date = datetime.strptime(since, "%Y-%m-%d").date()
        if until:
            until_date = datetime.strptime(until, "%Y-%m-%d").date()
        else:
            until_date = _current_et_date()
        update_jobs[job_id]["status"] = "running"
        async with AsyncSessionLocal() as db:
            result = await _run_games_ingest(
                since_date,
                until_date,
                season,
                include_team_stats,
                db,
                job_id=job_id,
            )
        update_jobs[job_id]["status"] = "completed"
        update_jobs[job_id]["result"] = result
        update_jobs[job_id]["finished_at"] = datetime.utcnow().isoformat()
    except Exception as e:
        logger.error(f"games ingest job {job_id} failed: {e}")
        update_jobs[job_id]["status"] = "failed"
        update_jobs[job_id]["error"] = str(e)
        update_jobs[job_id]["finished_at"] = datetime.utcnow().isoformat()


@router.post("/games/ingest/start")
async def start_games_ingest(
    since: str,
    until: str | None = None,
    season: str = "2025-26",
    include_team_stats: bool = True,
):
    try:
        since_date = datetime.strptime(since, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="since must be YYYY-MM-DD")
    if until:
        try:
            until_date = datetime.strptime(until, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="until must be YYYY-MM-DD")
    else:
        until_date = _current_et_date()

    if since_date > until_date:
        raise HTTPException(status_code=400, detail="since must be on or before until")

    job_id = str(uuid.uuid4())
    update_jobs[job_id] = {
        "job_id": job_id,
        "type": "games_ingest",
        "status": "queued",
        "since": since,
        "until": until or str(until_date),
        "season": season,
        "include_team_stats": include_team_stats,
        "games_done": 0,
        "games_total": None,
        "current_game_id": None,
        "current_game_date": None,
        "created_at": datetime.utcnow().isoformat(),
        "result": None,
        "error": None,
    }
    asyncio.create_task(
        _run_games_ingest_job(job_id, since, until, season, include_team_stats)
    )
    return {"status": "queued", "job_id": job_id}


@router.post("/games/ingest/latest/start")
async def start_latest_completed_games_ingest(
    season: str = "2025-26",
    include_team_stats: bool = True,
):
    target_date = _latest_completed_nba_et_date()
    since = str(target_date)
    until = str(target_date)

    job_id = str(uuid.uuid4())
    update_jobs[job_id] = {
        "job_id": job_id,
        "type": "games_ingest_latest",
        "status": "queued",
        "since": since,
        "until": until,
        "season": season,
        "include_team_stats": include_team_stats,
        "games_done": 0,
        "games_total": None,
        "current_game_id": None,
        "current_game_date": None,
        "created_at": datetime.utcnow().isoformat(),
        "result": None,
        "error": None,
    }
    asyncio.create_task(
        _run_games_ingest_job(job_id, since, until, season, include_team_stats)
    )
    return {
        "status": "queued",
        "job_id": job_id,
        "target_et_date": since,
        "season": season,
        "include_team_stats": include_team_stats,
    }


async def _run_nightly_pipeline_job(
    job_id: str,
    season: str,
    include_team_stats: bool,
    fallback_refresh: bool,
):
    stat_types = ["points", "assists", "rebounds", "threept", "threepa"]
    steps_total = 6 + len(stat_types)

    def set_step(step_name: str, step_index: int):
        update_jobs[job_id]["status"] = "running"
        update_jobs[job_id]["current_step"] = step_name
        update_jobs[job_id]["steps_done"] = step_index
        update_jobs[job_id]["steps_total"] = steps_total

    try:
        target_date = _latest_completed_nba_et_date()
        update_jobs[job_id]["target_et_date"] = str(target_date)

        async with AsyncSessionLocal() as db:
            set_step("game_ingest", 0)
            ingest_result = await _run_games_ingest(
                target_date,
                target_date,
                season,
                include_team_stats,
                db,
                job_id=job_id,
            )
            update_jobs[job_id]["ingest_result"] = ingest_result

            set_step("team_games_update", 1)
            team_games_result = await update_team_games_all_teams(season=season, db=db)
            update_jobs[job_id]["team_games_result"] = team_games_result

            set_step("player_team_refresh", 2)
            refresh_result = await refresh_player_team_abbr(
                db=db,
                season=season,
                fallback=fallback_refresh,
            )
            update_jobs[job_id]["refresh_result"] = refresh_result

        set_step("evaluate_actuals", 3)
        evaluate_result = {
            "points": await run_in_threadpool(update_prediction_actuals, sync_engine, "points"),
            "assists": await run_in_threadpool(update_prediction_actuals, sync_engine, "assists"),
            "rebounds": await run_in_threadpool(update_prediction_actuals, sync_engine, "rebounds"),
            "minutes": await run_in_threadpool(update_prediction_actuals, sync_engine, "minutes"),
            "threept": await run_in_threadpool(update_prediction_actuals, sync_engine, "threept"),
            "threepa": await run_in_threadpool(update_prediction_actuals, sync_engine, "threepa"),
        }
        update_jobs[job_id]["evaluate_result"] = evaluate_result

        set_step("rolling_update", 4)
        await run_in_threadpool(update_rolling_stats, sync_engine)

        async with AsyncSessionLocal() as db:
            set_step("under_risk_recalc", 5)
            under_risk_result = await recalc_under_risk_all(db=db)
            update_jobs[job_id]["under_risk_result"] = under_risk_result

        backtest_results: dict[str, dict] = {}
        for idx, stat_type in enumerate(stat_types, start=6):
            set_step(f"backtest_{stat_type}", idx)
            df_preds = await run_in_threadpool(
                walk_forward_backtest, sync_engine, stat_type, 15, None
            )
            if df_preds.empty:
                backtest_results[stat_type] = {"status": "no_data"}
                continue
            await run_in_threadpool(
                log_predictions,
                sync_engine,
                df_preds,
                stat_type,
                "walkforward",
                True,
            )
            updated = await run_in_threadpool(
                update_prediction_actuals, sync_engine, stat_type
            )
            backtest_results[stat_type] = {
                "status": "logged",
                "rows": int(len(df_preds)),
                "actuals_updated": updated,
            }
        update_jobs[job_id]["backtest_results"] = backtest_results

        update_jobs[job_id]["status"] = "completed"
        update_jobs[job_id]["steps_done"] = steps_total
        update_jobs[job_id]["current_step"] = "completed"
        update_jobs[job_id]["finished_at"] = datetime.utcnow().isoformat()
    except Exception as e:
        logger.error(f"nightly pipeline job {job_id} failed: {e}")
        update_jobs[job_id]["status"] = "failed"
        update_jobs[job_id]["error"] = str(e)
        update_jobs[job_id]["finished_at"] = datetime.utcnow().isoformat()


@router.post("/pipeline/nightly/start")
async def start_nightly_pipeline(
    season: str = "2025-26",
    include_team_stats: bool = True,
    fallback_refresh: bool = True,
):
    target_date = _latest_completed_nba_et_date()
    job_id = str(uuid.uuid4())
    update_jobs[job_id] = {
        "job_id": job_id,
        "type": "nightly_pipeline",
        "status": "queued",
        "season": season,
        "include_team_stats": include_team_stats,
        "fallback_refresh": fallback_refresh,
        "target_et_date": str(target_date),
        "steps_done": 0,
        "steps_total": 11,
        "current_step": "queued",
        "games_done": 0,
        "games_total": None,
        "current_game_id": None,
        "current_game_date": None,
        "created_at": datetime.utcnow().isoformat(),
        "result": None,
        "error": None,
    }
    asyncio.create_task(
        _run_nightly_pipeline_job(
            job_id,
            season,
            include_team_stats,
            fallback_refresh,
        )
    )
    return {
        "status": "queued",
        "job_id": job_id,
        "target_et_date": str(target_date),
        "season": season,
        "include_team_stats": include_team_stats,
        "fallback_refresh": fallback_refresh,
    }


@router.post("/last-n/update")
async def update_last_n_games_since(
    since: str,
    until: str | None = None,
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
    if until:
        try:
            until_date = datetime.strptime(until, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="until must be YYYY-MM-DD")
    else:
        # Use NBA league day boundary (US Eastern) to avoid AU/UTC date drift.
        until_date = datetime.now(ZoneInfo("America/New_York")).date()

    if since_date > until_date:
        raise HTTPException(status_code=400, detail="since must be on or before until")

    return await _run_last_n_update(since_date, until_date, season, db)


async def _run_last_n_update_job(
    job_id: str, since: str, until: str | None, season: str
):
    try:
        since_date = datetime.strptime(since, "%Y-%m-%d").date()
        if until:
            until_date = datetime.strptime(until, "%Y-%m-%d").date()
        else:
            until_date = datetime.now(ZoneInfo("America/New_York")).date()
        update_jobs[job_id]["status"] = "running"
        async with AsyncSessionLocal() as db:
            result = await _run_last_n_update(
                since_date, until_date, season, db, job_id=job_id
            )
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
    until: str | None = None,
    season: str = "2025-26",
):
    """
    Start last-n update in the background and return a job id for polling.
    """
    try:
        since_date = datetime.strptime(since, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="since must be YYYY-MM-DD")
    if until:
        try:
            until_date = datetime.strptime(until, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="until must be YYYY-MM-DD")
    else:
        until_date = datetime.now(ZoneInfo("America/New_York")).date()

    if since_date > until_date:
        raise HTTPException(status_code=400, detail="since must be on or before until")

    job_id = str(uuid.uuid4())
    update_jobs[job_id] = {
        "job_id": job_id,
        "type": "last_n_update",
        "status": "queued",
        "since": since,
        "until": until or str(until_date),
        "season": season,
        "players_done": 0,
        "players_total": None,
        "created_at": datetime.utcnow().isoformat(),
        "result": None,
        "error": None,
    }
    asyncio.create_task(_run_last_n_update_job(job_id, since, until, season))
    return {"status": "queued", "job_id": job_id}


@router.get("/jobs/{job_id}")
async def get_update_job_status(job_id: str):
    job = update_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@router.get("/ingestion-runs/latest")
async def get_latest_ingestion_run(db: AsyncSession = Depends(get_db)):
    row = (
        (
            await db.execute(
                select(
                    IngestionRun.id,
                    IngestionRun.ingest_type,
                    IngestionRun.since_date,
                    IngestionRun.season,
                    IngestionRun.status,
                    IngestionRun.created_at,
                )
                .order_by(IngestionRun.created_at.desc())
                .limit(1)
            )
        )
        .mappings()
        .first()
    )
    latest_game_date = (
        await db.execute(select(func.max(PlayerGameStat.game_date)))
    ).scalar_one_or_none()
    if not row:
        return {"status": "empty", "data": None, "latest_game_date": latest_game_date}
    return {"status": "ok", "data": dict(row), "latest_game_date": latest_game_date}


@router.get("/player-games/recent-dates")
async def get_recent_player_game_dates(limit: int = 5, db: AsyncSession = Depends(get_db)):
    safe_limit = max(1, min(limit, 20))
    rows = (
        (
            await db.execute(
                text(
                    """
                    SELECT id, player_id, game_id, game_date, matchup, points, assists, rebounds
                    FROM player_game_stats
                    ORDER BY game_date DESC, id DESC
                    LIMIT :limit
                    """
                ),
                {"limit": safe_limit},
            )
        )
        .mappings()
        .all()
    )
    return {"status": "ok", "data": [dict(r) for r in rows]}


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


@router.post("/players/refresh-team-abbr")
async def refresh_player_team_abbr(
    db: AsyncSession = Depends(get_db),
    limit: int | None = None,
    season: str = "2025-26",
    fallback: bool = True,
):
    """
    Refresh team_abbreviation for all active players using bulk stats first,
    then optional per-player fallback. Updates existing players and creates
    missing records when possible.
    """
    active_players = players.get_active_players()
    if limit:
        active_players = active_players[:limit]

    team_map: dict[int, str] = {}
    bulk_rows = 0
    bulk_resolved = 0
    bulk_failed = False

    try:
        bulk_df = nba_client.fetch_player_stats(
            season=season,
            per_mode_detailed="PerGame",
            season_type_all_star="Regular Season",
        )
        if (
            bulk_df is not None
            and not bulk_df.empty
            and "PLAYER_ID" in bulk_df.columns
            and "TEAM_ABBREVIATION" in bulk_df.columns
        ):
            bulk_rows = int(len(bulk_df))
            for _, row in bulk_df[["PLAYER_ID", "TEAM_ABBREVIATION"]].iterrows():
                pid = row.get("PLAYER_ID")
                team_abbr = row.get("TEAM_ABBREVIATION")
                if pd.notnull(pid) and pd.notnull(team_abbr):
                    team_map[int(pid)] = str(team_abbr)
            bulk_resolved = len(team_map)
        else:
            bulk_failed = True
    except Exception as e:
        logger.warning(f"Bulk player team refresh failed: {e}")
        bulk_failed = True

    updated = 0
    unchanged = 0
    created = 0
    skipped = 0
    failed = 0
    fallback_used = 0
    bulk_used = 0

    for p in active_players:
        player_id = p["id"]
        player_name = p["full_name"]
        team_abbr = team_map.get(int(player_id)) if team_map else None

        if team_abbr:
            bulk_used += 1
        elif fallback:
            fallback_used += 1
            for attempt in range(3):
                try:
                    async with throttler:
                        info_df, _ = nba_client.fetch_player_info(player_id)
                    if not info_df.empty and "TEAM_ABBREVIATION" in info_df.columns:
                        team_abbr = info_df.iloc[0]["TEAM_ABBREVIATION"]
                    else:
                        team_abbr = None
                    break
                except Exception as e:
                    logger.warning(
                        f"Attempt {attempt + 1} failed for player info {player_name}: {e}"
                    )
                    await asyncio.sleep(0.5)
            else:
                failed += 1
                await asyncio.sleep(0.05)
                continue

        if not team_abbr:
            skipped += 1
            await asyncio.sleep(0.05)
            continue

        for attempt in range(3):
            try:
                player = await db.get(Player, player_id)
                if not player:
                    player = Player(
                        id=player_id,
                        full_name=player_name,
                        team_abbreviation=team_abbr,
                    )
                    db.add(player)
                    created += 1
                elif player.team_abbreviation != team_abbr:
                    player.team_abbreviation = team_abbr
                    updated += 1
                else:
                    unchanged += 1
                break
            except Exception as e:
                logger.warning(
                    f"Attempt {attempt + 1} failed for player upsert {player_name}: {e}"
                )
                await asyncio.sleep(0.5)
        else:
            failed += 1

        await asyncio.sleep(0.05)

    await db.commit()

    return {
        "status": "completed",
        "players_targeted": len(active_players),
        "players_updated": updated,
        "players_unchanged": unchanged,
        "players_created": created,
        "players_skipped": skipped,
        "players_failed": failed,
        "bulk_rows": bulk_rows,
        "bulk_resolved": bulk_resolved,
        "bulk_used": bulk_used,
        "fallback_used": fallback_used,
        "bulk_failed": bulk_failed,
        "season": season,
        "fallback": fallback,
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
        raise HTTPException(
            status_code=500,
            detail="Failed to load teams. Please try again later.",
        )


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
        raise HTTPException(
            status_code=500,
            detail="Failed to load schedule. Please try again later.",
        )


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
        raise HTTPException(
            status_code=500,
            detail="Failed to recalculate under-risk. Please try again later.",
        )


@router.post("/under-risk/recalc-all")
async def recalc_under_risk_all(
    window_n: int = 20,
    db: AsyncSession = Depends(get_db),
):
    """
    Recalculate under-risk for all supported stat types.
    """
    stat_types = ["points", "assists", "rebounds", "threept", "threepa"]
    results = {}

    for stat_type in stat_types:
        try:
            results[stat_type] = await compute_under_risk(db, stat_type, window_n)
        except Exception as e:
            logger.error(f"Error computing under-risk for {stat_type}: {e}")
            results[stat_type] = {
                "status": "error",
                "detail": "Failed to compute under-risk.",
            }

    return {
        "status": "completed",
        "window_n": window_n,
        "results": results,
    }
