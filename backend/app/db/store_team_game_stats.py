from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime
from app.models.team_game_stat import TeamGameStat


async def save_team_game_stats(team_id: int, df, db: AsyncSession) -> int:
    inserted = 0

    if df is None or df.empty:
        return 0

    for _, row in df.iterrows():
        game_id = row.get("Game_ID") or row.get("GAME_ID")
        if not game_id:
            continue

        existing = await db.execute(
            select(TeamGameStat).where(
                TeamGameStat.team_id == team_id,
                TeamGameStat.game_id == str(game_id),
            )
        )
        if existing.scalar_one_or_none():
            continue

        game_date_raw = row.get("GAME_DATE")
        game_date = (
            datetime.strptime(game_date_raw, "%b %d, %Y").date()
            if game_date_raw
            else None
        )
        if not game_date:
            continue

        db.add(
            TeamGameStat(
                team_id=team_id,
                team_abbreviation=row.get("TEAM_ABBREVIATION"),
                game_id=str(game_id),
                game_date=game_date,
                matchup=row.get("MATCHUP"),
                points=row.get("PTS"),
                assists=row.get("AST"),
                rebounds=row.get("REB"),
                turnovers=row.get("TOV"),
            )
        )
        inserted += 1

    if inserted:
        await db.commit()

    return inserted
