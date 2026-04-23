from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

PLANNED_MARKETS = [
    "pitcher_strikeouts",
    "batter_hits",
    "batter_total_bases",
    "batter_home_runs",
]


@router.get("/markets")
def get_mlb_markets():
    return {
        "sport": "mlb",
        "status": "planned",
        "markets": PLANNED_MARKETS,
    }


@router.get("/{market}")
def get_mlb_predictions(
    market: str,
    day: str = Query("today", enum=["today", "tomorrow", "yesterday", "auto"]),
):
    if market not in PLANNED_MARKETS:
        raise HTTPException(status_code=404, detail="Unknown MLB market.")
    raise HTTPException(
        status_code=501,
        detail={
            "sport": "mlb",
            "market": market,
            "day": day,
            "status": "not_implemented",
            "message": "MLB prediction models have not been wired yet.",
        },
    )
