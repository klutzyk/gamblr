from sqlalchemy import Column, Integer, Float, Text, Date, DateTime, ForeignKey, UniqueConstraint
from app.db.base import Base


class PlayerUnderRisk(Base):
    __tablename__ = "player_under_risk"

    id = Column(Integer, primary_key=True)

    player_id = Column(Integer, ForeignKey("players.id", ondelete="CASCADE"))
    stat_type = Column(Text, nullable=False)

    window_n = Column(Integer, nullable=False)
    sample_size = Column(Integer, nullable=False)
    under_count = Column(Integer, nullable=False)
    under_rate = Column(Float, nullable=False)
    threshold_type = Column(Text, nullable=False)

    as_of_date = Column(Date, nullable=True)
    computed_at = Column(DateTime, nullable=False)

    __table_args__ = (
        UniqueConstraint("player_id", "stat_type", name="uq_player_under_risk"),
    )
