# API routes for the-odds-api.com provider
from fastapi import APIRouter, HTTPException
from app.services.theodds_client import TheOddsClient

router = APIRouter()

client = TheOddsClient()


# get all sports supported
@router.get("/sports")
async def list_sports():
    try:
        return await client.get_sports()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# get all odds for the upcoming games for today (US time)
@router.get("/{sport}")
async def get_odds(
    sport: str,
    regions: str = "us",
    markets: str = "h2h",
):
    try:
        return await client.get_odds(
            sport=sport,
            regions=regions,
            markets=markets,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# get all event details for today
@router.get("/{sport}/events")
async def get_events(sport: str):
    try:
        return await client.get_events(sport)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# get odds for a specific event (player props)
@router.get("/{sport}/events/{event_id}/odds")
async def get_event_odds(
    sport: str,
    event_id: str,
    regions: str = "us",
    markets: str = "player_points",
):
    try:
        return await client.get_event_odds(
            sport=sport,
            event_id=event_id,
            regions=regions,
            markets=markets,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
