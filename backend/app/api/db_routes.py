from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.services.theodds_client import TheOddsClient
from app.services.nba_client import NBAClient
from app.db.store_odds import save_event_odds
from app.db.store_player_game_stats import save_last_n_games
from nba_api.stats.static import players
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

    #  run the fetch and saveing of plauers in parallel for every active player
    tasks = [fetch_player_and_save(p, season, db) for p in active_players]
    results = await asyncio.gather(*tasks)

    # tracking variables (for verification)
    saved = results.count("saved")
    skipped = results.count("skipped")
    failed = results.count("failed")

    return {
        "status": "completed",
        "players_total": len(active_players),
        "players_saved": saved,
        "players_skipped": skipped,
        "players_failed": failed,
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

    await save_last_n_games(
        player_id=player_id,
        player_name=player_name,
        team_abbr=team_abbr,
        df=df,
        db=db,
    )

    return {"status": "saved", "games": min(len(df), MAX_GAMES_PER_PLAYER)}


# helper to fetch and store players in db implementing retry logic and rate limiting
async def fetch_player_and_save(player, season, db):
    player_id = player["id"]
    player_name = player["full_name"]

    # retry 3 times if fetching fails
    for attempt in range(3):
        try:
            # throttling to 5 requests per second
            async with throttler:
                df = nba_client.fetch_player_game_log(player_id, season)
            break  # on success, exit retry loop
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {player_name}: {e}")
            await asyncio.sleep(0.5)  # wait a bit before retry
    else:
        logger.error(f"All retries failed for {player_name}")
        return "failed"

    if df.empty:
        return "skipped"

    team_abbr = (
        df.iloc[0]["TEAM_ABBREVIATION"] if "TEAM_ABBREVIATION" in df.columns else None
    )

    await save_last_n_games(
        player_id=player_id, player_name=player_name, team_abbr=team_abbr, df=df, db=db
    )
    return "saved"
