from sqlalchemy import Column, Integer, Text, Float, ForeignKey
from app.db.base import Base


class PlayerProp(Base):
    __tablename__ = "player_props"

    id = Column(Integer, primary_key=True)
    market_id = Column(Integer, ForeignKey("markets.id", ondelete="CASCADE"))

    player_name = Column(Text, nullable=False)  # description
    side = Column(Text, nullable=False)  # Over / Under

    price = Column(Float, nullable=False)
    line = Column(Float, nullable=False)  # points line (22.5 etc)
