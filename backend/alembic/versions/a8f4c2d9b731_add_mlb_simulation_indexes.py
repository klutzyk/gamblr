"""add mlb simulation indexes

Revision ID: a8f4c2d9b731
Revises: 5f1a8c2e7b44
Create Date: 2026-04-27 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op


revision: str = "a8f4c2d9b731"
down_revision: Union[str, Sequence[str], None] = "5f1a8c2e7b44"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index("ix_mlb_pitch_events_pitcher", "mlb_pitch_events", ["pitcher_id"])
    op.create_index("ix_mlb_pitch_events_pitcher_type", "mlb_pitch_events", ["pitcher_id", "pitch_type_code"])
    op.create_index("ix_mlb_pitch_events_batter", "mlb_pitch_events", ["batter_id"])
    op.create_index("ix_mlb_batted_ball_events_batter", "mlb_batted_ball_events", ["batter_id"])
    op.create_index("ix_mlb_batted_ball_events_pitcher", "mlb_batted_ball_events", ["pitcher_id"])
    op.create_index("ix_mlb_player_game_batting_player", "mlb_player_game_batting", ["player_id"])
    op.create_index("ix_mlb_player_game_pitching_player", "mlb_player_game_pitching", ["player_id"])
    op.create_index("ix_mlb_player_game_pitching_team", "mlb_player_game_pitching", ["team_id"])
    op.create_index("ix_mlb_weather_snapshots_game_time", "mlb_weather_snapshots", ["game_pk", "target_time_utc"])
    op.create_index("ix_mlb_prediction_logs_game_team", "mlb_prediction_logs", ["game_pk", "team_id"])


def downgrade() -> None:
    op.drop_index("ix_mlb_prediction_logs_game_team", table_name="mlb_prediction_logs")
    op.drop_index("ix_mlb_weather_snapshots_game_time", table_name="mlb_weather_snapshots")
    op.drop_index("ix_mlb_player_game_pitching_team", table_name="mlb_player_game_pitching")
    op.drop_index("ix_mlb_player_game_pitching_player", table_name="mlb_player_game_pitching")
    op.drop_index("ix_mlb_player_game_batting_player", table_name="mlb_player_game_batting")
    op.drop_index("ix_mlb_batted_ball_events_pitcher", table_name="mlb_batted_ball_events")
    op.drop_index("ix_mlb_batted_ball_events_batter", table_name="mlb_batted_ball_events")
    op.drop_index("ix_mlb_pitch_events_batter", table_name="mlb_pitch_events")
    op.drop_index("ix_mlb_pitch_events_pitcher_type", table_name="mlb_pitch_events")
    op.drop_index("ix_mlb_pitch_events_pitcher", table_name="mlb_pitch_events")
