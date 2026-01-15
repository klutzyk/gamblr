# API routes for the-odds-api.com provider
from fastapi import APIRouter, HTTPException
from app.services.theodds_client import TheOddsClient

router = APIRouter()

client = TheOddsClient()


@router.get("/sports")
async def list_sports():
    try:
        return await client.get_sports()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
