from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import create_engine

from app.core.config import settings
from app.services.jedibets_first_basket_client import JediBetsFirstBasketClient
from app.services.lineup_resolver import LineupResolver
from app.services.rotowire_lineups_client import RotoWireLineupsClient

router = APIRouter()
client = RotoWireLineupsClient(timeout=20)
jedi_client = JediBetsFirstBasketClient(timeout=20)
sync_engine = create_engine(settings.DATABASE_URL.replace("+asyncpg", ""))
resolver = LineupResolver(sync_engine)


@router.get("/rotowire")
def get_rotowire_lineups(
    day: str | None = Query(
        None,
        description="Optional slate date hint: today|tomorrow|yesterday",
    ),
    resolve_ids: bool = Query(
        True,
        description="Resolve Rotowire starter names to players.id in local DB.",
    ),
):
    try:
        payload = client.fetch_lineups(day=day)
        if not resolve_ids:
            return payload
        return resolver.enrich_rotowire_payload(payload)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch lineups: {exc}")


@router.get("/jedibets/first-basket-stats")
def get_jedibets_first_basket_stats():
    try:
        return jedi_client.fetch_stats()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch JediBets stats: {exc}")
