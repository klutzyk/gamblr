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
    regions: str | None = "us",
    bookmakers: str | None = "fanduel",
    markets: str = "h2h",
    min_remaining_after_call: int = 3,
):
    try:
        return await client.get_odds(
            sport=sport,
            regions=regions,
            bookmakers=bookmakers,
            markets=markets,
            min_remaining_after_call=min_remaining_after_call,
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
    regions: str | None = "us",
    bookmakers: str | None = None,
    markets: str = "player_points",
    min_remaining_after_call: int = 3,
):
    try:
        return await client.get_event_odds(
            sport=sport,
            event_id=event_id,
            regions=regions,
            bookmakers=bookmakers,
            markets=markets,
            min_remaining_after_call=min_remaining_after_call,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/usage/snapshot")
async def usage_snapshot():
    return client.latest_usage()
