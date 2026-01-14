# Main FastAPI application
from fastapi import FastAPI
from app.api import health, player_stats, player_props
from app.db.base import Base
from app.db.session import engine
import logging

# for logging in fastapi
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(name)s: %(message)s",
)


app = FastAPI(title="NBA Betting API")

app.include_router(health.router)
app.include_router(player_stats.router, prefix="/players", tags=["Players"])
app.include_router(player_props.router, prefix="/player-props", tags=["Player Props"])


@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@app.get("/")
def root():
    return {"message": "NBA Betting API running"}
