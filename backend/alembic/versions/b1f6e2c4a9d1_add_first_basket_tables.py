"""add first basket tables

Revision ID: b1f6e2c4a9d1
Revises: 2abd7d80ba59
Create Date: 2026-02-04 02:30:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b1f6e2c4a9d1"
down_revision: Union[str, Sequence[str], None] = "2abd7d80ba59"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "first_basket_labels",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("game_date", sa.Date(), nullable=False),
        sa.Column("season", sa.Text(), nullable=True),
        sa.Column("home_team_id", sa.Integer(), nullable=True),
        sa.Column("away_team_id", sa.Integer(), nullable=True),
        sa.Column("home_team_abbr", sa.Text(), nullable=True),
        sa.Column("away_team_abbr", sa.Text(), nullable=True),
        sa.Column("first_scoring_team_id", sa.Integer(), nullable=True),
        sa.Column("first_scoring_team_abbr", sa.Text(), nullable=True),
        sa.Column("first_scorer_player_id", sa.Integer(), nullable=True),
        sa.Column("first_scorer_name", sa.Text(), nullable=True),
        sa.Column("first_score_event_num", sa.Integer(), nullable=True),
        sa.Column("first_score_seconds", sa.Float(), nullable=True),
        sa.Column("first_score_action_type", sa.Text(), nullable=True),
        sa.Column("first_score_description", sa.Text(), nullable=True),
        sa.Column("winning_jump_ball_team_id", sa.Integer(), nullable=True),
        sa.Column("winning_jump_ball_team_abbr", sa.Text(), nullable=True),
        sa.Column("jump_ball_home_player_id", sa.Integer(), nullable=True),
        sa.Column("jump_ball_away_player_id", sa.Integer(), nullable=True),
        sa.Column("jump_ball_winner_player_id", sa.Integer(), nullable=True),
        sa.Column("home_starter_ids_json", sa.Text(), nullable=True),
        sa.Column("away_starter_ids_json", sa.Text(), nullable=True),
        sa.Column("is_valid_label", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("source", sa.Text(), nullable=False, server_default="nba_api"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["away_team_id"], ["teams.id"]),
        sa.ForeignKeyConstraint(["first_scorer_player_id"], ["players.id"]),
        sa.ForeignKeyConstraint(["first_scoring_team_id"], ["teams.id"]),
        sa.ForeignKeyConstraint(["home_team_id"], ["teams.id"]),
        sa.ForeignKeyConstraint(["jump_ball_away_player_id"], ["players.id"]),
        sa.ForeignKeyConstraint(["jump_ball_home_player_id"], ["players.id"]),
        sa.ForeignKeyConstraint(["jump_ball_winner_player_id"], ["players.id"]),
        sa.ForeignKeyConstraint(["winning_jump_ball_team_id"], ["teams.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("game_id", name="uq_first_basket_label_game_id"),
    )
    op.create_index("ix_first_basket_labels_game_date", "first_basket_labels", ["game_date"])
    op.create_index("ix_first_basket_labels_first_scorer", "first_basket_labels", ["first_scorer_player_id"])

    op.create_table(
        "first_basket_prediction_logs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("game_id", sa.Text(), nullable=False),
        sa.Column("game_date", sa.Date(), nullable=True),
        sa.Column("prediction_date", sa.Date(), nullable=True),
        sa.Column("player_id", sa.Integer(), nullable=False),
        sa.Column("team_id", sa.Integer(), nullable=True),
        sa.Column("team_abbreviation", sa.Text(), nullable=True),
        sa.Column("first_basket_prob", sa.Float(), nullable=False),
        sa.Column("team_scores_first_prob", sa.Float(), nullable=True),
        sa.Column("player_share_on_team", sa.Float(), nullable=True),
        sa.Column("model_version", sa.Text(), nullable=True),
        sa.Column("lineup_status", sa.Text(), nullable=True),
        sa.Column("actual_first_basket", sa.Integer(), nullable=True),
        sa.Column("abs_error", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["player_id"], ["players.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["team_id"], ["teams.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("game_id", "player_id", name="uq_first_basket_prediction_game_player"),
    )
    op.create_index("ix_fb_pred_logs_game_date", "first_basket_prediction_logs", ["game_date"])
    op.create_index("ix_fb_pred_logs_player_id", "first_basket_prediction_logs", ["player_id"])


def downgrade() -> None:
    op.drop_index("ix_fb_pred_logs_player_id", table_name="first_basket_prediction_logs")
    op.drop_index("ix_fb_pred_logs_game_date", table_name="first_basket_prediction_logs")
    op.drop_table("first_basket_prediction_logs")

    op.drop_index("ix_first_basket_labels_first_scorer", table_name="first_basket_labels")
    op.drop_index("ix_first_basket_labels_game_date", table_name="first_basket_labels")
    op.drop_table("first_basket_labels")
