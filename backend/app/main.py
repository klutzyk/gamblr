# Main FastAPI application
from fastapi import FastAPI
from app.api import health
from app.db.base import Base
from app.db.session import engine

app = FastAPI(title="NBA Betting API")

app.include_router(health.router)

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@app.get("/")
def root():
    return {"message": "NBA Betting API running"}
