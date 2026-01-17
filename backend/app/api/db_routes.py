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

logger = logging.getLogger(__name__)

router = APIRouter()
odds_client = TheOddsClient()
nba_client = NBAClient()


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
    season: str = "2025-26",
    db: AsyncSession = Depends(get_db),
):
    active_players = players.get_active_players()

    saved = 0
    skipped = 0
    failed = 0

    for p in active_players:
        player_id = p["id"]
        player_name = p["full_name"]

        try:
            df = nba_client.fetch_player_game_log(player_id, season)

            if df.empty:
                skipped += 1
                continue

            team_abbr = (
                df.iloc[0]["TEAM_ABBREVIATION"]
                if "TEAM_ABBREVIATION" in df.columns
                else None
            )

            await save_last_n_games(
                player_id=player_id,
                player_name=player_name,
                team_abbr=team_abbr,
                df=df,
                db=db,
            )

            saved += 1
            await asyncio.sleep(0.2)

        except Exception as e:
            logger.error(f"Failed player {player_id}: {e}")
            failed += 1
            await asyncio.sleep(0.2)

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

    return {"status": "saved", "games": min(len(df), 20)}
