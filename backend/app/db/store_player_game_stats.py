from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from datetime import datetime
from app.models.player import Player
from app.models.player_game_stat import PlayerGameStat
from app.core.constants import MAX_GAMES_PER_PLAYER


# Save the last n games (N takes on the value of MAX_GAMES_PER_PLAYER constant)
# If player data already exists in the db, insert only missing games
# Remove old records until each player has MAX_GAMES_PER_PLAYER records
async def save_last_n_games(
    player_id: int,
    player_name: str,
    team_abbr: str,
    df,
    db: AsyncSession,
):
    # Ensure player exists
    player = await db.get(Player, player_id)
    if not player:
        player = Player(
            id=player_id,
            full_name=player_name,
            team_abbreviation=team_abbr,
        )
        db.add(player)
        await db.flush()

    # Normalize API dates
    df["GAME_DATE"] = df["GAME_DATE"].apply(
        lambda d: datetime.strptime(d, "%b %d, %Y").date()
    )

    # Load existing games for update checks
    existing_rows_result = await db.execute(
        select(PlayerGameStat).where(PlayerGameStat.player_id == player_id)
    )
    existing_rows = {row[0].game_id: row[0] for row in existing_rows_result.all()}

    if df.empty:
        return 0  # nothing to process

    inserted = 0
    updated = 0
    # Insert new games or update missing shooting fields
    for _, row in df.iterrows():
        game_id = row["Game_ID"]
        existing = existing_rows.get(game_id)
        if existing:
            has_updates = False
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
                updated += 1
            continue

        db.add(
            PlayerGameStat(
                player_id=player_id,
                game_id=game_id,
                game_date=row["GAME_DATE"],
                matchup=row["MATCHUP"],
                minutes=row["MIN"],
                points=row["PTS"],
                assists=row["AST"],
                rebounds=row["REB"],
                steals=row["STL"],
                blocks=row["BLK"],
                turnovers=row["TOV"],
                fgm=row.get("FGM"),
                fga=row.get("FGA"),
                fg3m=row.get("FG3M"),
                fg3a=row.get("FG3A"),
            )
        )
        inserted += 1

    await db.commit()

    # Enforce rolling window (keep newest N and delete old)
    if MAX_GAMES_PER_PLAYER and MAX_GAMES_PER_PLAYER > 0:
        await db.execute(
            delete(PlayerGameStat)
            .where(PlayerGameStat.player_id == player_id)
            .where(
                PlayerGameStat.game_id.notin_(
                    select(PlayerGameStat.game_id)
                    .where(PlayerGameStat.player_id == player_id)
                    .order_by(PlayerGameStat.game_date.desc())
                    .limit(MAX_GAMES_PER_PLAYER)
                )
            )
        )

        await db.commit()

    return inserted + updated
