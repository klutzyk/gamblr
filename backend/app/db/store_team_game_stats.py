from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime
from app.models.team_game_stat import TeamGameStat


async def save_team_game_stats(
    team_id: int,
    df,
    db: AsyncSession,
    team_abbr: str | None = None,
    last_game_date=None,
) -> int:
    inserted = 0

    if df is None or df.empty:
        return 0

    if last_game_date is not None and "GAME_DATE" in df.columns:
        df["GAME_DATE"] = df["GAME_DATE"].apply(
            lambda d: datetime.strptime(d, "%b %d, %Y").date()
        )
        df = df[df["GAME_DATE"] > last_game_date]

    for _, row in df.iterrows():
        game_id = row.get("Game_ID") or row.get("GAME_ID")
        if not game_id:
            continue

        if last_game_date is None:
            existing = await db.execute(
                select(TeamGameStat).where(
                    TeamGameStat.team_id == team_id,
                    TeamGameStat.game_id == str(game_id),
                )
            )
            if existing.scalar_one_or_none():
                continue

        game_date = row.get("GAME_DATE")
        if isinstance(game_date, str):
            game_date = datetime.strptime(game_date, "%b %d, %Y").date()
        if not game_date:
            continue

        resolved_abbr = row.get("TEAM_ABBREVIATION") or team_abbr
        if not resolved_abbr and row.get("MATCHUP"):
            resolved_abbr = str(row.get("MATCHUP")).split(" ")[0]

        db.add(
            TeamGameStat(
                team_id=team_id,
                team_abbreviation=resolved_abbr,
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
