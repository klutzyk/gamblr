from sqlalchemy import Column, Text, Integer, Date, ForeignKey, UniqueConstraint
from app.db.base import Base


class GameSchedule(Base):
    __tablename__ = "game_schedule"

    game_id = Column(Text, primary_key=True)
    game_date = Column(Date, nullable=False)
    season = Column(Text, nullable=False)
    season_type = Column(Text)  # e.g., "Regular Season" or "Playoffs"

    home_team_id = Column(Integer, ForeignKey("teams.id"))
    away_team_id = Column(Integer, ForeignKey("teams.id"))

    home_team_abbr = Column(Text)
    away_team_abbr = Column(Text)

    matchup = Column(Text)

    __table_args__ = (UniqueConstraint("game_id", name="uq_game_id"),)
