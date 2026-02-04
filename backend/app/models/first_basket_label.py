from sqlalchemy import (
    Boolean,
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


class FirstBasketLabel(Base):
    __tablename__ = "first_basket_labels"

    id = Column(Integer, primary_key=True)

    game_id = Column(Text, nullable=False)
    game_date = Column(Date, nullable=False)
    season = Column(Text, nullable=True)

    home_team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    away_team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    home_team_abbr = Column(Text, nullable=True)
    away_team_abbr = Column(Text, nullable=True)

    first_scoring_team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    first_scoring_team_abbr = Column(Text, nullable=True)
    first_scorer_player_id = Column(Integer, ForeignKey("players.id"), nullable=True)
    first_scorer_name = Column(Text, nullable=True)

    first_score_event_num = Column(Integer, nullable=True)
    first_score_seconds = Column(Float, nullable=True)
    first_score_action_type = Column(Text, nullable=True)
    first_score_description = Column(Text, nullable=True)

    winning_jump_ball_team_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    winning_jump_ball_team_abbr = Column(Text, nullable=True)

    jump_ball_home_player_id = Column(Integer, ForeignKey("players.id"), nullable=True)
    jump_ball_away_player_id = Column(Integer, ForeignKey("players.id"), nullable=True)
    jump_ball_winner_player_id = Column(Integer, ForeignKey("players.id"), nullable=True)

    home_starter_ids_json = Column(Text, nullable=True)
    away_starter_ids_json = Column(Text, nullable=True)
    is_valid_label = Column(Boolean, nullable=False, default=True)

    source = Column(Text, nullable=False, default="nba_api")
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)

    __table_args__ = (
        UniqueConstraint("game_id", name="uq_first_basket_label_game_id"),
    )
