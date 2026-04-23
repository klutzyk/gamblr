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
    updated = 0

    if df is None or df.empty:
        return 0

    def first_value(row, keys):
        for key in keys:
            if key in row and row.get(key) is not None:
                return row.get(key)
        return None

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
        existing_row = existing.scalar_one_or_none()

        off_rating = first_value(row, ["OFF_RTG", "OFF_RATING"])
        def_rating = first_value(row, ["DEF_RTG", "DEF_RATING"])
        net_rating = first_value(row, ["NET_RTG", "NET_RATING"])
        pace = first_value(row, ["PACE"])
        ast_pct = first_value(row, ["AST_PCT"])
        reb_pct = first_value(row, ["REB_PCT"])

        if existing_row:
            if existing_row.off_rating is None and off_rating is not None:
                existing_row.off_rating = off_rating
                updated += 1
            if existing_row.def_rating is None and def_rating is not None:
                existing_row.def_rating = def_rating
                updated += 1
            if existing_row.net_rating is None and net_rating is not None:
                existing_row.net_rating = net_rating
                updated += 1
            if existing_row.pace is None and pace is not None:
                existing_row.pace = pace
                updated += 1
            if existing_row.ast_pct is None and ast_pct is not None:
                existing_row.ast_pct = ast_pct
                updated += 1
            if existing_row.reb_pct is None and reb_pct is not None:
                existing_row.reb_pct = reb_pct
                updated += 1
            continue

        db.add(
            LineupStat(
                team_id=team_id,
                season=season,
                lineup_id=str(lineup_id),
                lineup=row.get("GROUP_NAME") or row.get("LINEUP"),
                minutes=row.get("MIN"),
                off_rating=off_rating,
                def_rating=def_rating,
                net_rating=net_rating,
                pace=pace,
                ast_pct=ast_pct,
                reb_pct=reb_pct,
            )
        )
        inserted += 1

    if inserted or updated:
        await db.commit()

    return inserted + updated
