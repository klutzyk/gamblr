from sqlalchemy import select, desc
from app.db.models import PlayerGameStats


# return the latest game date for a given player
async def get_latest_game_date(player_id: int, db):
    result = await db.execute(
        select(PlayerGameStats.game_date)
        .where(PlayerGameStats.player_id == player_id)
        .order_by(desc(PlayerGameStats.game_date))
        .limit(1)
    )
    row = result.first()
    return row[0] if row else None
