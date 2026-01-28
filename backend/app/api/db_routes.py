from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.services.theodds_client import TheOddsClient
from app.services.nba_client import NBAClient
from app.db.store_odds import save_event_odds
from app.db.store_player_game_stats import save_last_n_games
from app.db.store_team_game_stats import save_team_game_stats
from app.db.store_lineup_stats import save_lineup_stats
from app.db.store_teams import load_teams
from app.db.store_schedule import load_schedule
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
from asyncio_throttle import Throttler
import pandas as pd
from app.core.constants import MAX_GAMES_PER_PLAYER

logger = logging.getLogger(__name__)

router = APIRouter()
odds_client = TheOddsClient()
nba_client = NBAClient()

#  throttler to prevent hammering NBA API. rate limiting to 5 requests/sec
throttler = Throttler(rate_limit=5, period=1)


# store player points props for a single event (game)
@router.post("/player-points/{event_id}")
async def refresh_player_points_event(
    event_id: str, db: AsyncSession = Depends(get_db)
):
    try:
        odds_data = await odds_client.get_event_odds(
            sport="basketball_nba", event_id=event_id
        )
        await save_event_odds(odds_data, db)
        return {
            "status": "success",
            "event_id": event_id,
            "stored_markets": len(odds_data.get("bookmakers", [])),
        }
    except Exception as e:
        logger.error(f"Error fetching/storing event {event_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# get all events for today and store all player point props for those events
# (MIGHT BE EXCESSIVE AND OVERUSE THE API)
@router.post("/player-points/all")
async def refresh_all_player_points(db: AsyncSession = Depends(get_db)):
    try:
        events = await odds_client.get_events("basketball_nba")
        total_markets = 0

        for event in events:
            event_id = event["id"]
            odds_data = await odds_client.get_event_odds(
                sport="basketball_nba", event_id=event_id
            )
            await save_event_odds(odds_data, db)
            total_markets += len(odds_data.get("bookmakers", []))

        return {
            "status": "success",
            "events_processed": len(events),
            "stored_markets": total_markets,
        }
    except Exception as e:
        logger.error(f"Error fetching/storing all events: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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

    schedule_result = await db.execute(
        select(
            GameSchedule.home_team_abbr,
            GameSchedule.away_team_abbr,
        ).where(GameSchedule.game_date > since_date)
    )
    teams_with_games = set()
    for home_abbr, away_abbr in schedule_result.all():
        if home_abbr:
            teams_with_games.add(home_abbr)
        if away_abbr:
            teams_with_games.add(away_abbr)

    if teams_with_games:
        players_result = await db.execute(
            select(Player).where(Player.team_abbreviation.in_(teams_with_games))
        )
        active_players = [
            {"id": p.id, "full_name": p.full_name}
            for p in players_result.scalars().all()
        ]
    else:
        active_players = players.get_active_players()
    saved = 0
    skipped = 0
    failed = 0
    total_new_games = 0

    for p in active_players:
        player_id = p["id"]
        player_name = p["full_name"]

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
        "since": since,
    }


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
