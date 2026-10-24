import pandas as pd
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.base import Base
from app.db.nba.store_player_game_stats import save_last_n_games
from app.models.player import Player  # noqa: F401
from app.models.player_game_stat import PlayerGameStat


@pytest.mark.asyncio
async def test_player_log_rerun_is_idempotent_and_preserves_game_team():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all, tables=[Player.__table__, PlayerGameStat.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    frame = pd.DataFrame([{
        "Game_ID": "0022600001", "GAME_DATE": "OCT 23, 2026", "MATCHUP": "BOS @ NYK",
        "MIN": 30, "PTS": 20, "AST": 5, "REB": 6, "STL": 1, "BLK": 0, "TOV": 2,
        "FGM": 8, "FGA": 15, "FG3M": 2, "FG3A": 5,
    }])
    async with sessions() as session:
        assert await save_last_n_games(1, "Test Player", "BOS", frame, session) == 1
        assert await save_last_n_games(1, "Test Player", "BOS", frame, session) == 0
        assert (await session.execute(select(func.count()).select_from(PlayerGameStat))).scalar_one() == 1
        row = (await session.execute(select(PlayerGameStat))).scalar_one()
        assert row.season == "2026-27"
        assert row.team_abbreviation == "BOS"

        corrected = frame.copy()
        corrected.loc[0, "PTS"] = 21
        assert await save_last_n_games(1, "Test Player", "BOS", corrected, session) == 1
        assert (await session.execute(select(func.count()).select_from(PlayerGameStat))).scalar_one() == 1
        row = (await session.execute(select(PlayerGameStat))).scalar_one()
        assert row.points == 21
    await engine.dispose()
