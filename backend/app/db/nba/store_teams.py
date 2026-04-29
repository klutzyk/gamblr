# app/db/store_teams.py
from sqlalchemy.ext.asyncio import AsyncSession
from nba_api.stats.static import teams as nba_teams
from app.models.team import Team


async def load_teams(db: AsyncSession):
    # using the static dataset from the nba_api
    all_teams = nba_teams.get_teams()

    inserted = 0
    skipped = 0

    for t in all_teams:
        team_id = t["id"]

        # check if already exists
        existing = await db.get(Team, team_id)
        if existing:
            skipped += 1
            continue

        db.add(
            Team(
                id=team_id,
                full_name=t["full_name"],
                abbreviation=t["abbreviation"],
                nickname=t.get("nickname"),
                city=t.get("city"),
                state=t.get("state"),
                year_founded=t.get("year_founded"),
            )
        )
        inserted += 1

    await db.commit()
    print(f"Teams loaded: {inserted} inserted, {skipped} skipped")
