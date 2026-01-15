from sqlalchemy import Column, Integer, Text
from app.db.base import Base


class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key=True)  # NBA player_id
    full_name = Column(Text, nullable=False)
    team_abbreviation = Column(Text)
