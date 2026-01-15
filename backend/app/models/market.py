from sqlalchemy import Column, Integer, Text, TIMESTAMP, ForeignKey
from app.db.base import Base


class Market(Base):
    __tablename__ = "markets"

    id = Column(Integer, primary_key=True)
    bookmaker_id = Column(Integer, ForeignKey("bookmakers.id", ondelete="CASCADE"))

    key = Column(Text, nullable=False)  # player_points
    last_update = Column(TIMESTAMP(timezone=True), nullable=False)
