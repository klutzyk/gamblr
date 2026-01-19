from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from datetime import datetime
from app.models.player import Player
from app.models.player_game_stat import PlayerGameStat
from app.core.constants import MAX_GAMES_PER_PLAYER


# Save the last n games (N takes on the value of MAX_GAMES_PER_PLAYER constant)
# If player data a;lready exists in the db, get the latest player game data and only insert the new records
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

    # Get latest stored game_date for this player
    result = await db.execute(
        select(PlayerGameStat.game_date)
        .where(PlayerGameStat.player_id == player_id)
        .order_by(PlayerGameStat.game_date.desc())
        .limit(1)
    )
    last_game_date = result.scalar_one_or_none()

    # Normalize API dates
    df["GAME_DATE"] = df["GAME_DATE"].apply(
        lambda d: datetime.strptime(d, "%b %d, %Y").date()
    )

    # Keep only th NEW games
    if last_game_date:
        df = df[df["GAME_DATE"] > last_game_date]

    if df.empty:
        return 0  # nothing new to insert

    # Insert only new games
    for _, row in df.iterrows():
        db.add(
            PlayerGameStat(
                player_id=player_id,
                game_id=row["Game_ID"],
                game_date=row["GAME_DATE"],
                matchup=row["MATCHUP"],
                minutes=row["MIN"],
                points=row["PTS"],
                assists=row["AST"],
                rebounds=row["REB"],
                steals=row["STL"],
                blocks=row["BLK"],
                turnovers=row["TOV"],
            )
        )

    await db.commit()

    # Enforce rolling window (keep newest N and delete old)
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

    return len(df)
