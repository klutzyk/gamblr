# API to get player prop data from sportsbooks (using sportsdata api https://sportsdata.io/)
from fastapi import APIRouter, HTTPException
from app.services.sportsdata_client import SportsDataClient

router = APIRouter()

client = SportsDataClient()


# Returns the player props for the given game
# Returns a BettingMarket[] object which includes BettingOutcome[]
# (https://sportsdata.io/developers/data-dictionary/nba#bettingentitymetadata)
@router.get("/{game_id}")
async def get_player_props(game_id: int):
    try:
        data = await client.get_player_props_by_game(game_id)
        return {"game_id": game_id, "markets": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
