# TO match the nba_api (from github)
from sqlalchemy import Column, Integer, Text, UniqueConstraint
from app.db.base import Base


class Team(Base):
    __tablename__ = "teams"

    id = Column(Integer, primary_key=True)  # NBA teamId
    full_name = Column(Text, nullable=False)  # "Los Angeles Lakers"
    city = Column(Text)  # "Los Angeles"
    nickname = Column(Text)  # "Lakers"
    abbreviation = Column(Text, nullable=False)  # "LAL"
    state = Column(Text)  # "CA"
    year_founded = Column(Integer)

    __table_args__ = (UniqueConstraint("abbreviation", name="uq_team_abbr"),)
