# app/db/store_schedule.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from nba_api.stats.endpoints import scheduleleaguev2
from datetime import datetime
from app.models.game_schedule import GameSchedule
from app.models.team import Team


async def load_schedule(db: AsyncSession, season: str):
    """
    Load ALL games for a season from NBA API.
    """
    sched = scheduleleaguev2.ScheduleLeagueV2(season=season)
    df_games = sched.season_games.get_data_frame()

    # Get all team IDs in DB (to check if team is nba team before insert)
    result = await db.execute(select(Team.id))
    team_ids_in_db = {row[0] for row in result.fetchall()}

    inserted = 0
    skipped = 0

    for _, row in df_games.iterrows():
        home_id = row["homeTeam_teamId"]
        away_id = row["awayTeam_teamId"]

        # skip if either team is not in DB
        if home_id not in team_ids_in_db or away_id not in team_ids_in_db:
            skipped += 1
            continue

        game_id = row["gameId"]

        # skip if already exists
        result = await db.execute(
            select(GameSchedule).where(GameSchedule.game_id == game_id)
        )
        if result.scalar_one_or_none():
            skipped += 1
            continue

        game_date = (
            datetime.strptime(row["gameDate"], "%m/%d/%Y %H:%M:%S").date()
            if "gameDate" in row and row["gameDate"]
            else datetime.strptime(row["gameDateEst"], "%Y-%m-%d").date()
        )

        db.add(
            GameSchedule(
                game_id=game_id,
                game_date=game_date,
                season=row["seasonYear"],
                season_type=row.get("gameSubtype", "Regular Season"),
                home_team_id=home_id,
                away_team_id=away_id,
                home_team_abbr=row["homeTeam_teamTricode"],
                away_team_abbr=row["awayTeam_teamTricode"],
                matchup=f"{row['awayTeam_teamTricode']} @ {row['homeTeam_teamTricode']}",
            )
        )
        inserted += 1

    await db.commit()
    print(f"Schedule loaded: {inserted} inserted, {skipped} skipped")
