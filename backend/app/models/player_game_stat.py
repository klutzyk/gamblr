from sqlalchemy import Column, Integer, Float, Text, Date, ForeignKey, UniqueConstraint
from app.db.base import Base


class PlayerGameStat(Base):
    __tablename__ = "player_game_stats"

    id = Column(Integer, primary_key=True)

    player_id = Column(Integer, ForeignKey("players.id", ondelete="CASCADE"))
    game_id = Column(Text, nullable=False)

    game_date = Column(Date, nullable=False)
    matchup = Column(Text)

    minutes = Column(Float)
    points = Column(Float)
    assists = Column(Float)
    rebounds = Column(Float)
    steals = Column(Float)
    blocks = Column(Float)
    turnovers = Column(Float)

    __table_args__ = (UniqueConstraint("player_id", "game_id", name="uq_player_game"),)
