from sqlalchemy import Column, Text, TIMESTAMP
from app.db.base import Base


class Event(Base):
    __tablename__ = "events"

    id = Column(Text, primary_key=True)  # event_id from the-odds-api
    sport_key = Column(Text, nullable=False)
    sport_title = Column(Text, nullable=False)

    commence_time = Column(TIMESTAMP(timezone=True), nullable=False)

    home_team = Column(Text, nullable=False)
    away_team = Column(Text, nullable=False)
