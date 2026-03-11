# API routes for the-odds-api.com provider
from fastapi import APIRouter, HTTPException
import logging
from app.services.theodds_client import TheOddsClient

router = APIRouter()
logger = logging.getLogger(__name__)

client = TheOddsClient()


def _raise_provider_error(action: str, exc: Exception):
    # Never return raw provider/client exceptions to callers; they may include
    # sensitive request details (for example query parameters).
    logger.exception("TheOdds provider error during %s", action)
    raise HTTPException(
        status_code=502,
        detail=f"Failed to {action}. Please try again later.",
    ) from exc


# get all sports supported
@router.get("/sports")
async def list_sports():
    try:
        return await client.get_sports()
    except Exception as exc:
        _raise_provider_error("fetch sports", exc)


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
    except Exception as exc:
        _raise_provider_error("fetch odds", exc)


# get all event details for today
@router.get("/{sport}/events")
async def get_events(sport: str):
    try:
        return await client.get_events(sport)
    except Exception as exc:
        _raise_provider_error("fetch events", exc)


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
    except Exception as exc:
        _raise_provider_error("fetch event odds", exc)


@router.get("/usage/snapshot")
async def usage_snapshot():
    return client.latest_usage()
