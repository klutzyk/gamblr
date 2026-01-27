from sqlalchemy import Column, Integer, Float, Text, Date, ForeignKey, UniqueConstraint
from app.db.base import Base


class PredictionLog(Base):
    __tablename__ = "prediction_logs"

    id = Column(Integer, primary_key=True)

    player_id = Column(Integer, ForeignKey("players.id", ondelete="CASCADE"))
    stat_type = Column(Text, nullable=False)
    game_id = Column(Text, nullable=True)
    game_date = Column(Date, nullable=True)
    prediction_date = Column(Date, nullable=True)

    pred_value = Column(Float, nullable=True)
    pred_p10 = Column(Float, nullable=True)
    pred_p50 = Column(Float, nullable=True)
    pred_p90 = Column(Float, nullable=True)
    confidence = Column(Float, nullable=True)
    model_version = Column(Text, nullable=True)

    actual_value = Column(Float, nullable=True)
    abs_error = Column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("player_id", "stat_type", "game_id", name="uq_pred_log"),
    )
