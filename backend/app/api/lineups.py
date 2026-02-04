from fastapi import APIRouter, HTTPException

from app.services.rotowire_lineups_client import RotoWireLineupsClient

router = APIRouter()
client = RotoWireLineupsClient(timeout=20)


@router.get("/rotowire")
def get_rotowire_lineups():
    try:
        return client.fetch_lineups()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch lineups: {exc}")
