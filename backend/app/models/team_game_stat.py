from sqlalchemy import Column, Integer, Float, Text, Date, ForeignKey, UniqueConstraint
from app.db.base import Base


class TeamGameStat(Base):
    __tablename__ = "team_game_stats"

    id = Column(Integer, primary_key=True)

    team_id = Column(Integer, ForeignKey("teams.id", ondelete="CASCADE"))
    team_abbreviation = Column(Text)
    game_id = Column(Text, nullable=False)

    game_date = Column(Date, nullable=False)
    matchup = Column(Text)

    points = Column(Float)
    assists = Column(Float)
    rebounds = Column(Float)
    turnovers = Column(Float)
    fgm = Column(Float)
    fga = Column(Float)
    fg3m = Column(Float)
    fg3a = Column(Float)

    __table_args__ = (UniqueConstraint("team_id", "game_id", name="uq_team_game"),)
