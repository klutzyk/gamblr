from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from app.models.player import Player
from app.models.player_game_stat import PlayerGameStat
from datetime import datetime
from app.core.constants import MAX_GAMES_PER_PLAYER


async def save_last_n_games(
    player_id: int,
    player_name: str,
    team_abbr: str,
    df,
    db: AsyncSession,
):
    # player row
    player = await db.get(Player, player_id)
    if not player:
        player = Player(
            id=player_id,
            full_name=player_name,
            team_abbreviation=team_abbr,
        )
        db.add(player)
        await db.flush()

    # keep only last N games from API upto the value of MAX_GAMES_PER_PLAYER (20 for now)
    df = df.sort_values("GAME_DATE", ascending=False).head(MAX_GAMES_PER_PLAYER)

    # delete existing games for this player
    delete_stmt = delete(PlayerGameStat).where(PlayerGameStat.player_id == player_id)
    await db.execute(delete_stmt)

    # insert fresh window
    for _, row in df.iterrows():
        game = PlayerGameStat(
            player_id=player_id,
            game_id=row["Game_ID"],
            game_date=datetime.strptime(row["GAME_DATE"], "%b %d, %Y").date(),
            matchup=row["MATCHUP"],
            minutes=row["MIN"],
            points=row["PTS"],
            assists=row["AST"],
            rebounds=row["REB"],
            steals=row["STL"],
            blocks=row["BLK"],
            turnovers=row["TOV"],
        )
        db.add(game)

    await db.commit()
