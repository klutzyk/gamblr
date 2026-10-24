# app/db/store_schedule.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from nba_api.stats.endpoints import scheduleleaguev2
from datetime import datetime
from app.models.game_schedule import GameSchedule
from app.models.team import Team
from app.core.nba_season import resolve_nba_season


def classify_season_type(row) -> str:
    """Normalize ScheduleLeagueV2 labels into stable phase names."""
    label = str(row.get("gameLabel") or "").strip().lower()
    subtype = str(row.get("gameSubtype") or "").strip().lower()
    game_id = str(row.get("gameId") or "")
    if "preseason" in label or game_id.startswith("001"):
        return "Preseason"
    if "all-star" in label or "all star" in label or game_id.startswith("003"):
        return "All-Star"
    if "play-in" in label or game_id.startswith("005"):
        return "Play-In"
    if "playoff" in label or game_id.startswith("004"):
        return "Playoffs"
    if subtype in {"in-season", "in-season-knockout"} or "nba cup" in label:
        return "Regular Season (NBA Cup)"
    return "Regular Season"


async def load_schedule(db: AsyncSession, season: str):
    """
    Load ALL games for a season from NBA API.
    """
    season = resolve_nba_season(season)
    sched = scheduleleaguev2.ScheduleLeagueV2(
        season=season,
        timeout=45,
    )
    df_games = sched.season_games.get_data_frame()

    # Get all team IDs in DB (to check if team is nba team before insert)
    result = await db.execute(select(Team.id))
    team_ids_in_db = {row[0] for row in result.fetchall()}

    inserted = 0
    updated = 0
    skipped = 0

    existing_result = await db.execute(
        select(GameSchedule).where(GameSchedule.season == season)
    )
    existing_by_id = {
        str(game.game_id): game for game in existing_result.scalars().all()
    }

    for _, row in df_games.iterrows():
        home_id = row["homeTeam_teamId"]
        away_id = row["awayTeam_teamId"]

        # skip if either team is not in DB
        if home_id not in team_ids_in_db or away_id not in team_ids_in_db:
            skipped += 1
            continue

        game_id = str(row["gameId"]).zfill(10)

        game_date = (
            datetime.strptime(row["gameDate"], "%m/%d/%Y %H:%M:%S").date()
            if "gameDate" in row and row["gameDate"]
            else datetime.strptime(row["gameDateEst"], "%Y-%m-%d").date()
        )

        values = {
            "game_date": game_date,
            "season": season,
            "season_type": classify_season_type(row),
            "home_team_id": int(home_id),
            "away_team_id": int(away_id),
            "home_team_abbr": row["homeTeam_teamTricode"],
            "away_team_abbr": row["awayTeam_teamTricode"],
            "matchup": f"{row['awayTeam_teamTricode']} @ {row['homeTeam_teamTricode']}",
        }
        existing = existing_by_id.get(game_id)
        if existing:
            changed = False
            for key, value in values.items():
                if getattr(existing, key) != value:
                    setattr(existing, key, value)
                    changed = True
            if changed:
                updated += 1
            else:
                skipped += 1
            continue

        db.add(
            GameSchedule(
                game_id=game_id,
                **values,
            )
        )
        inserted += 1

    await db.commit()
    return {
        "season": season,
        "provider_rows": int(len(df_games)),
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
    }
