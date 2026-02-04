from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Text,
    UniqueConstraint,
)

from app.db.base import Base


class FirstBasketPredictionLog(Base):
    __tablename__ = "first_basket_prediction_logs"

    id = Column(Integer, primary_key=True)

    game_id = Column(Text, nullable=False)
    game_date = Column(Date, nullable=True)
    prediction_date = Column(Date, nullable=True)

    player_id = Column(Integer, ForeignKey("players.id", ondelete="CASCADE"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    team_abbreviation = Column(Text, nullable=True)

    first_basket_prob = Column(Float, nullable=False)
    team_scores_first_prob = Column(Float, nullable=True)
    player_share_on_team = Column(Float, nullable=True)
    model_version = Column(Text, nullable=True)
    lineup_status = Column(Text, nullable=True)

    actual_first_basket = Column(Integer, nullable=True)
    abs_error = Column(Float, nullable=True)

    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)

    __table_args__ = (
        UniqueConstraint("game_id", "player_id", name="uq_first_basket_prediction_game_player"),
    )
