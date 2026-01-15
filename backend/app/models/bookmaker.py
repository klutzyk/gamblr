from sqlalchemy import Column, Integer, Text, ForeignKey
from app.db.base import Base


class Bookmaker(Base):
    __tablename__ = "bookmakers"

    id = Column(Integer, primary_key=True)
    event_id = Column(Text, ForeignKey("events.id", ondelete="CASCADE"))

    key = Column(Text, nullable=False)  # fanduel
    title = Column(Text, nullable=False)  # FanDuel
