from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.lineup_stat import LineupStat


async def save_lineup_stats(
    team_id: int,
    season: str,
    df,
    db: AsyncSession,
) -> int:
    inserted = 0

    if df is None or df.empty:
        return 0

    for _, row in df.iterrows():
        lineup_id = row.get("GROUP_ID") or row.get("LINEUP_ID")
        if not lineup_id:
            continue

        existing = await db.execute(
            select(LineupStat).where(
                LineupStat.team_id == team_id,
                LineupStat.season == season,
                LineupStat.lineup_id == str(lineup_id),
            )
        )
        if existing.scalar_one_or_none():
            continue

        db.add(
            LineupStat(
                team_id=team_id,
                season=season,
                lineup_id=str(lineup_id),
                lineup=row.get("GROUP_NAME") or row.get("LINEUP"),
                minutes=row.get("MIN"),
                off_rating=row.get("OFF_RTG"),
                def_rating=row.get("DEF_RTG"),
                net_rating=row.get("NET_RTG"),
                pace=row.get("PACE"),
                ast_pct=row.get("AST_PCT"),
                reb_pct=row.get("REB_PCT"),
            )
        )
        inserted += 1

    if inserted:
        await db.commit()

    return inserted
