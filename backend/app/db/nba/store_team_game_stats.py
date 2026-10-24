from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime
import pandas as pd
from app.models.team_game_stat import TeamGameStat
from app.core.nba_season import season_for_date


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
        df = df.copy()
        df["GAME_DATE"] = df["GAME_DATE"].apply(
            lambda d: pd.to_datetime(d, errors="raise").date()
        )
        df = df[df["GAME_DATE"] > last_game_date]

    # Map existing rows for in-place updates when new columns are added.
    existing_rows = {}
    if last_game_date is None:
        existing_result = await db.execute(
            select(TeamGameStat).where(TeamGameStat.team_id == team_id)
        )
        existing_rows = {row[0].game_id: row[0] for row in existing_result.all()}

    for _, row in df.iterrows():
        game_id = row.get("Game_ID") or row.get("GAME_ID")
        if not game_id:
            continue

        existing = existing_rows.get(str(game_id))
        if existing:
            has_updates = False
            existing_date = existing.game_date
            if not existing.season and existing_date:
                existing.season = season_for_date(existing_date)
                has_updates = True
            for col, key in [
                ("fgm", "FGM"),
                ("fga", "FGA"),
                ("fg3m", "FG3M"),
                ("fg3a", "FG3A"),
            ]:
                value = row.get(key)
                if value is not None and getattr(existing, col) is None:
                    setattr(existing, col, value)
                    has_updates = True
            if has_updates:
                inserted += 1
            continue

        game_date = row.get("GAME_DATE")
        if isinstance(game_date, str):
            game_date = pd.to_datetime(game_date, errors="raise").date()
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
                season=season_for_date(game_date),
                matchup=row.get("MATCHUP"),
                points=row.get("PTS"),
                assists=row.get("AST"),
                rebounds=row.get("REB"),
                turnovers=row.get("TOV"),
                fgm=row.get("FGM"),
                fga=row.get("FGA"),
                fg3m=row.get("FG3M"),
                fg3a=row.get("FG3A"),
            )
        )
        inserted += 1

    if inserted:
        await db.commit()

    return inserted
