# app/db/store_schedule.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from nba_api.stats.endpoints import scheduleleaguev2
from datetime import datetime
from app.models.game_schedule import GameSchedule


async def load_schedule(db: AsyncSession, season: str):
    """
    Load ALL games for a season from NBA API.
    """
    sched = scheduleleaguev2.ScheduleLeagueV2(season=season)
    df_games = sched.season_games.get_data_frame()

    inserted = 0
    skipped = 0

    for _, row in df_games.iterrows():
        game_id = row["gameId"]

        # skip if already exists
        result = await db.execute(
            select(GameSchedule).where(GameSchedule.game_id == game_id)
        )
        if result.scalar_one_or_none():
            skipped += 1
            continue

        game_date = (
            datetime.strptime(row["gameDate"], "%b %d, %Y").date()
            if "gameDate" in row
            else datetime.strptime(row["gameDateEst"], "%Y-%m-%d").date()
        )

        db.add(
            GameSchedule(
                game_id=game_id,
                game_date=game_date,
                season=row["seasonYear"],
                season_type=row.get("gameSubtype", "Regular Season"),
                home_team_id=row["homeTeam_teamId"],
                away_team_id=row["awayTeam_teamId"],
                home_team_abbr=row["homeTeam_teamTricode"],
                away_team_abbr=row["awayTeam_teamTricode"],
                matchup=f"{row['awayTeam_teamTricode']} @ {row['homeTeam_teamTricode']}",
            )
        )
        inserted += 1

    await db.commit()
    print(f"Schedule loaded: {inserted} inserted, {skipped} skipped")
