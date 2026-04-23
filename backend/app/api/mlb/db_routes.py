from fastapi import APIRouter, HTTPException, Query

router = APIRouter()


@router.post("/schedule/load")
async def load_mlb_schedule(
    season: str = Query(..., description="MLB season identifier, e.g. 2026"),
):
    raise HTTPException(
        status_code=501,
        detail={
            "sport": "mlb",
            "status": "not_implemented",
            "message": "MLB schedule ingestion has not been implemented yet.",
            "season": season,
        },
    )


@router.post("/games/ingest")
async def ingest_mlb_games(
    since: str | None = Query(None),
    until: str | None = Query(None),
):
    raise HTTPException(
        status_code=501,
        detail={
            "sport": "mlb",
            "status": "not_implemented",
            "message": "MLB game ingestion has not been implemented yet.",
            "since": since,
            "until": until,
        },
    )
