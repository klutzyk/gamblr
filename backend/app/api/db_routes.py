from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.services.theodds_client import TheOddsClient
from app.db.store_odds import save_event_odds
from app.db.store_player_game_stats import save_last_5_games
import logging

logger = logging.getLogger(__name__)

router = APIRouter()
client = TheOddsClient()


# store player points props for a single event (game)
@router.post("/player-points/{event_id}")
async def refresh_player_points_event(
    event_id: str, db: AsyncSession = Depends(get_db)
):
    try:
        odds_data = await client.get_event_odds(
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
        events = await client.get_events("basketball_nba")
        total_markets = 0

        for event in events:
            event_id = event["id"]
            odds_data = await client.get_event_odds(
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


# get and store the last 5 box score stats for the givn player
@router.post("/last-5/{player_id}")
async def store_last_5_games(
    player_id: int,
    season: str = "2025-26",
    db: AsyncSession = Depends(get_db),
):
    df = client.fetch_player_game_log(player_id, season)

    if df.empty:
        return {"status": "no data"}

    await save_last_5_games(
        player_id=player_id,
        player_name=df.iloc[0]["PLAYER_NAME"],
        team_abbr=df.iloc[0]["TEAM_ABBREVIATION"],
        df=df,
        db=db,
    )

    return {"status": "saved", "games": len(df.head(5))}
