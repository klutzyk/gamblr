# Main FastAPI application
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import health, player_stats, player_props, theodds, db_routes
from app.db.base import Base
from app.db.session import engine

import logging

# for logging in fastapi
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(name)s: %(message)s",
)


app = FastAPI(title="NBA Betting API")

# Allow frontend dev server to call the API for testing (NEED TO CHANGE THIS LATER!!!)
origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(player_stats.router, prefix="/players", tags=["Players"])
app.include_router(player_props.router, prefix="/player-props", tags=["Player Props"])
app.include_router(theodds.router, prefix="/odds", tags=["Odds API"])
app.include_router(db_routes.router, prefix="/db", tags=["DB Storage"])
app.include_router(db_routes.router, prefix="/db", tags=["DB Storage"])
# @app.on_event("startup")
# async def startup():
#     async with engine.begin() as conn:
#         await conn.run_sync(Base.metadata.create_all)


@app.get("/")
def root():
    return {"message": "NBA Betting API running"}
