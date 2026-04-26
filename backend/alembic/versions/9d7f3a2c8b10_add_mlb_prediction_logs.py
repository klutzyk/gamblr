"""add mlb prediction logs

Revision ID: 9d7f3a2c8b10
Revises: 6e2c9b8a4f12
Create Date: 2026-04-26 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "9d7f3a2c8b10"
down_revision: Union[str, Sequence[str], None] = "6e2c9b8a4f12"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "mlb_prediction_logs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("market", sa.Text(), nullable=False),
        sa.Column("game_pk", sa.BigInteger(), nullable=False),
        sa.Column("game_date", sa.Date(), nullable=False),
        sa.Column("prediction_date", sa.Date(), nullable=False),
        sa.Column("player_id", sa.Integer(), nullable=False),
        sa.Column("player_name", sa.Text(), nullable=True),
        sa.Column("team_id", sa.Integer(), nullable=True),
        sa.Column("team_abbreviation", sa.Text(), nullable=True),
        sa.Column("opponent_team_id", sa.Integer(), nullable=True),
        sa.Column("opponent_team_abbreviation", sa.Text(), nullable=True),
        sa.Column("is_home", sa.Boolean(), nullable=True),
        sa.Column("batting_order", sa.Float(), nullable=True),
        sa.Column("has_posted_lineup", sa.Boolean(), nullable=True),
        sa.Column("starter_pitcher_id", sa.Integer(), nullable=True),
        sa.Column("probability", sa.Float(), nullable=True),
        sa.Column("prediction", sa.Float(), nullable=True),
        sa.Column("model_path", sa.Text(), nullable=True),
        sa.Column("actual_value", sa.Float(), nullable=True),
        sa.Column("abs_error", sa.Float(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["game_pk"], ["mlb_games.game_pk"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["opponent_team_id"], ["mlb_teams.id"]),
        sa.ForeignKeyConstraint(["player_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["starter_pitcher_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["team_id"], ["mlb_teams.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "market",
            "game_pk",
            "player_id",
            name="uq_mlb_prediction_log_market_game_player",
        ),
    )
    op.create_index("ix_mlb_prediction_logs_date_market", "mlb_prediction_logs", ["game_date", "market"])
    op.create_index("ix_mlb_prediction_logs_player", "mlb_prediction_logs", ["player_id"])


def downgrade() -> None:
    op.drop_index("ix_mlb_prediction_logs_player", table_name="mlb_prediction_logs")
    op.drop_index("ix_mlb_prediction_logs_date_market", table_name="mlb_prediction_logs")
    op.drop_table("mlb_prediction_logs")
