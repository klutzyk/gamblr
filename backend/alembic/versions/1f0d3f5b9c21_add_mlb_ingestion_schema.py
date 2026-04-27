"""add mlb ingestion schema

Revision ID: 1f0d3f5b9c21
Revises: cc3f1c2d7a01, 7a74b57c08ca
Create Date: 2026-04-23 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "1f0d3f5b9c21"
down_revision: Union[str, Sequence[str], None] = ("cc3f1c2d7a01", "7a74b57c08ca")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "mlb_source_pulls",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("resource_type", sa.Text(), nullable=False),
        sa.Column("request_url", sa.Text(), nullable=False),
        sa.Column("request_params", sa.JSON(), nullable=True),
        sa.Column("local_path", sa.Text(), nullable=False),
        sa.Column("response_format", sa.Text(), nullable=False),
        sa.Column("season", sa.Integer(), nullable=True),
        sa.Column("game_pk", sa.BigInteger(), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("status", sa.Text(), nullable=False, server_default="completed"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_mlb_source_pulls_source_resource", "mlb_source_pulls", ["source", "resource_type"])
    op.create_index("ix_mlb_source_pulls_game_pk", "mlb_source_pulls", ["game_pk"])
    op.create_index("ix_mlb_source_pulls_season", "mlb_source_pulls", ["season"])

    op.create_table(
        "mlb_venues",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("city", sa.Text(), nullable=True),
        sa.Column("state", sa.Text(), nullable=True),
        sa.Column("country", sa.Text(), nullable=True),
        sa.Column("timezone_id", sa.Text(), nullable=True),
        sa.Column("timezone_offset", sa.Integer(), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("elevation", sa.Integer(), nullable=True),
        sa.Column("roof_type", sa.Text(), nullable=True),
        sa.Column("turf_type", sa.Text(), nullable=True),
        sa.Column("left_line", sa.Integer(), nullable=True),
        sa.Column("left_center", sa.Integer(), nullable=True),
        sa.Column("center", sa.Integer(), nullable=True),
        sa.Column("right_center", sa.Integer(), nullable=True),
        sa.Column("right_line", sa.Integer(), nullable=True),
        sa.Column("capacity", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "mlb_teams",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("abbreviation", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("team_name", sa.Text(), nullable=True),
        sa.Column("location_name", sa.Text(), nullable=True),
        sa.Column("franchise_name", sa.Text(), nullable=True),
        sa.Column("club_name", sa.Text(), nullable=True),
        sa.Column("league_id", sa.Integer(), nullable=True),
        sa.Column("division_id", sa.Integer(), nullable=True),
        sa.Column("venue_id", sa.Integer(), nullable=True),
        sa.Column("first_year_of_play", sa.Text(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.ForeignKeyConstraint(["venue_id"], ["mlb_venues.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("abbreviation", name="uq_mlb_teams_abbreviation"),
    )

    op.create_table(
        "mlb_players",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("full_name", sa.Text(), nullable=False),
        sa.Column("first_name", sa.Text(), nullable=True),
        sa.Column("last_name", sa.Text(), nullable=True),
        sa.Column("use_name", sa.Text(), nullable=True),
        sa.Column("use_last_name", sa.Text(), nullable=True),
        sa.Column("birth_date", sa.Date(), nullable=True),
        sa.Column("current_age", sa.Integer(), nullable=True),
        sa.Column("bat_side", sa.Text(), nullable=True),
        sa.Column("pitch_hand", sa.Text(), nullable=True),
        sa.Column("primary_position_code", sa.Text(), nullable=True),
        sa.Column("primary_position_name", sa.Text(), nullable=True),
        sa.Column("primary_position_abbreviation", sa.Text(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("draft_year", sa.Integer(), nullable=True),
        sa.Column("mlb_debut_date", sa.Date(), nullable=True),
        sa.Column("last_played_date", sa.Date(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "mlb_games",
        sa.Column("game_pk", sa.BigInteger(), nullable=False),
        sa.Column("official_date", sa.Date(), nullable=False),
        sa.Column("start_time_utc", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("season", sa.Integer(), nullable=False),
        sa.Column("game_type", sa.Text(), nullable=True),
        sa.Column("double_header", sa.Text(), nullable=True),
        sa.Column("game_number", sa.Integer(), nullable=True),
        sa.Column("status_code", sa.Text(), nullable=True),
        sa.Column("detailed_state", sa.Text(), nullable=True),
        sa.Column("day_night", sa.Text(), nullable=True),
        sa.Column("home_team_id", sa.Integer(), nullable=False),
        sa.Column("away_team_id", sa.Integer(), nullable=False),
        sa.Column("venue_id", sa.Integer(), nullable=True),
        sa.Column("home_score", sa.Integer(), nullable=True),
        sa.Column("away_score", sa.Integer(), nullable=True),
        sa.Column("probable_home_pitcher_id", sa.Integer(), nullable=True),
        sa.Column("probable_away_pitcher_id", sa.Integer(), nullable=True),
        sa.Column("weather_condition", sa.Text(), nullable=True),
        sa.Column("temperature_f", sa.Float(), nullable=True),
        sa.Column("wind_text", sa.Text(), nullable=True),
        sa.Column("roof_type", sa.Text(), nullable=True),
        sa.Column("last_ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["away_team_id"], ["mlb_teams.id"]),
        sa.ForeignKeyConstraint(["home_team_id"], ["mlb_teams.id"]),
        sa.ForeignKeyConstraint(["probable_away_pitcher_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["probable_home_pitcher_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["venue_id"], ["mlb_venues.id"]),
        sa.PrimaryKeyConstraint("game_pk"),
    )
    op.create_index("ix_mlb_games_official_date", "mlb_games", ["official_date"])
    op.create_index("ix_mlb_games_season", "mlb_games", ["season"])

    op.create_table(
        "mlb_game_snapshots",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("game_pk", sa.BigInteger(), nullable=False),
        sa.Column("source_pull_id", sa.Integer(), nullable=True),
        sa.Column("snapshot_type", sa.Text(), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status_code", sa.Text(), nullable=True),
        sa.Column("detailed_state", sa.Text(), nullable=True),
        sa.Column("weather_condition", sa.Text(), nullable=True),
        sa.Column("temperature_f", sa.Float(), nullable=True),
        sa.Column("wind_text", sa.Text(), nullable=True),
        sa.Column("roof_type", sa.Text(), nullable=True),
        sa.Column("probable_home_pitcher_id", sa.Integer(), nullable=True),
        sa.Column("probable_away_pitcher_id", sa.Integer(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["game_pk"], ["mlb_games.game_pk"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["probable_away_pitcher_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["probable_home_pitcher_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["source_pull_id"], ["mlb_source_pulls.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_mlb_game_snapshots_game_type_captured",
        "mlb_game_snapshots",
        ["game_pk", "snapshot_type", "captured_at"],
    )

    op.create_table(
        "mlb_lineup_snapshots",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("snapshot_id", sa.Integer(), nullable=False),
        sa.Column("game_pk", sa.BigInteger(), nullable=False),
        sa.Column("team_id", sa.Integer(), nullable=False),
        sa.Column("player_id", sa.Integer(), nullable=False),
        sa.Column("batting_order", sa.Integer(), nullable=True),
        sa.Column("position_code", sa.Text(), nullable=True),
        sa.Column("position_abbreviation", sa.Text(), nullable=True),
        sa.Column("status_code", sa.Text(), nullable=True),
        sa.Column("status_description", sa.Text(), nullable=True),
        sa.Column("is_starter", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("is_bench", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("is_substitute", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.ForeignKeyConstraint(["game_pk"], ["mlb_games.game_pk"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["player_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["snapshot_id"], ["mlb_game_snapshots.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["team_id"], ["mlb_teams.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("snapshot_id", "team_id", "player_id", name="uq_mlb_lineup_snapshot_player"),
    )
    op.create_index(
        "ix_mlb_lineup_snapshots_game_team_order",
        "mlb_lineup_snapshots",
        ["game_pk", "team_id", "batting_order"],
    )

    op.create_table(
        "mlb_player_game_batting",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("game_pk", sa.BigInteger(), nullable=False),
        sa.Column("player_id", sa.Integer(), nullable=False),
        sa.Column("team_id", sa.Integer(), nullable=False),
        sa.Column("batting_order", sa.Integer(), nullable=True),
        sa.Column("plate_appearances", sa.Integer(), nullable=True),
        sa.Column("at_bats", sa.Integer(), nullable=True),
        sa.Column("hits", sa.Integer(), nullable=True),
        sa.Column("doubles", sa.Integer(), nullable=True),
        sa.Column("triples", sa.Integer(), nullable=True),
        sa.Column("home_runs", sa.Integer(), nullable=True),
        sa.Column("total_bases", sa.Integer(), nullable=True),
        sa.Column("runs", sa.Integer(), nullable=True),
        sa.Column("rbi", sa.Integer(), nullable=True),
        sa.Column("walks", sa.Integer(), nullable=True),
        sa.Column("strikeouts", sa.Integer(), nullable=True),
        sa.Column("hit_by_pitch", sa.Integer(), nullable=True),
        sa.Column("stolen_bases", sa.Integer(), nullable=True),
        sa.Column("caught_stealing", sa.Integer(), nullable=True),
        sa.Column("left_on_base", sa.Integer(), nullable=True),
        sa.Column("sac_bunts", sa.Integer(), nullable=True),
        sa.Column("sac_flies", sa.Integer(), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["game_pk"], ["mlb_games.game_pk"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["player_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["team_id"], ["mlb_teams.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("game_pk", "player_id", name="uq_mlb_player_game_batting"),
    )
    op.create_index("ix_mlb_player_game_batting_game_team", "mlb_player_game_batting", ["game_pk", "team_id"])

    op.create_table(
        "mlb_player_game_pitching",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("game_pk", sa.BigInteger(), nullable=False),
        sa.Column("player_id", sa.Integer(), nullable=False),
        sa.Column("team_id", sa.Integer(), nullable=False),
        sa.Column("is_starter", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("innings_pitched", sa.Text(), nullable=True),
        sa.Column("outs_recorded", sa.Integer(), nullable=True),
        sa.Column("batters_faced", sa.Integer(), nullable=True),
        sa.Column("pitches_thrown", sa.Integer(), nullable=True),
        sa.Column("strikes", sa.Integer(), nullable=True),
        sa.Column("balls", sa.Integer(), nullable=True),
        sa.Column("hits_allowed", sa.Integer(), nullable=True),
        sa.Column("home_runs_allowed", sa.Integer(), nullable=True),
        sa.Column("earned_runs", sa.Integer(), nullable=True),
        sa.Column("walks", sa.Integer(), nullable=True),
        sa.Column("strikeouts", sa.Integer(), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["game_pk"], ["mlb_games.game_pk"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["player_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["team_id"], ["mlb_teams.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("game_pk", "player_id", name="uq_mlb_player_game_pitching"),
    )
    op.create_index("ix_mlb_player_game_pitching_game_team", "mlb_player_game_pitching", ["game_pk", "team_id"])

    op.create_table(
        "mlb_pitch_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("game_pk", sa.BigInteger(), nullable=False),
        sa.Column("play_id", sa.Text(), nullable=False),
        sa.Column("at_bat_index", sa.Integer(), nullable=False),
        sa.Column("pitch_number", sa.Integer(), nullable=True),
        sa.Column("inning", sa.Integer(), nullable=True),
        sa.Column("half_inning", sa.Text(), nullable=True),
        sa.Column("pitcher_id", sa.Integer(), nullable=True),
        sa.Column("batter_id", sa.Integer(), nullable=True),
        sa.Column("balls_before", sa.Integer(), nullable=True),
        sa.Column("strikes_before", sa.Integer(), nullable=True),
        sa.Column("outs_before", sa.Integer(), nullable=True),
        sa.Column("balls_after", sa.Integer(), nullable=True),
        sa.Column("strikes_after", sa.Integer(), nullable=True),
        sa.Column("outs_after", sa.Integer(), nullable=True),
        sa.Column("pitch_type_code", sa.Text(), nullable=True),
        sa.Column("pitch_type_description", sa.Text(), nullable=True),
        sa.Column("call_code", sa.Text(), nullable=True),
        sa.Column("call_description", sa.Text(), nullable=True),
        sa.Column("event_type", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_in_play", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("is_strike", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("is_ball", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("is_out", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("start_speed", sa.Float(), nullable=True),
        sa.Column("end_speed", sa.Float(), nullable=True),
        sa.Column("zone", sa.Integer(), nullable=True),
        sa.Column("plate_time", sa.Float(), nullable=True),
        sa.Column("extension", sa.Float(), nullable=True),
        sa.Column("spin_rate", sa.Float(), nullable=True),
        sa.Column("spin_direction", sa.Float(), nullable=True),
        sa.Column("break_angle", sa.Float(), nullable=True),
        sa.Column("break_length", sa.Float(), nullable=True),
        sa.Column("break_y", sa.Float(), nullable=True),
        sa.Column("break_vertical", sa.Float(), nullable=True),
        sa.Column("break_vertical_induced", sa.Float(), nullable=True),
        sa.Column("break_horizontal", sa.Float(), nullable=True),
        sa.Column("pfx_x", sa.Float(), nullable=True),
        sa.Column("pfx_z", sa.Float(), nullable=True),
        sa.Column("plate_x", sa.Float(), nullable=True),
        sa.Column("plate_z", sa.Float(), nullable=True),
        sa.Column("release_pos_x", sa.Float(), nullable=True),
        sa.Column("release_pos_y", sa.Float(), nullable=True),
        sa.Column("release_pos_z", sa.Float(), nullable=True),
        sa.Column("vx0", sa.Float(), nullable=True),
        sa.Column("vy0", sa.Float(), nullable=True),
        sa.Column("vz0", sa.Float(), nullable=True),
        sa.Column("ax", sa.Float(), nullable=True),
        sa.Column("ay", sa.Float(), nullable=True),
        sa.Column("az", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(["batter_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["game_pk"], ["mlb_games.game_pk"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["pitcher_id"], ["mlb_players.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("game_pk", "play_id", name="uq_mlb_pitch_event_play"),
    )
    op.create_index(
        "ix_mlb_pitch_events_game_inning",
        "mlb_pitch_events",
        ["game_pk", "inning", "half_inning"],
    )
    op.create_index(
        "ix_mlb_pitch_events_pitcher_batter",
        "mlb_pitch_events",
        ["pitcher_id", "batter_id"],
    )

    op.create_table(
        "mlb_batted_ball_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("game_pk", sa.BigInteger(), nullable=False),
        sa.Column("play_id", sa.Text(), nullable=False),
        sa.Column("at_bat_index", sa.Integer(), nullable=False),
        sa.Column("inning", sa.Integer(), nullable=True),
        sa.Column("half_inning", sa.Text(), nullable=True),
        sa.Column("pitcher_id", sa.Integer(), nullable=True),
        sa.Column("batter_id", sa.Integer(), nullable=True),
        sa.Column("event_type", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("launch_speed", sa.Float(), nullable=True),
        sa.Column("launch_angle", sa.Float(), nullable=True),
        sa.Column("total_distance", sa.Float(), nullable=True),
        sa.Column("trajectory", sa.Text(), nullable=True),
        sa.Column("hardness", sa.Text(), nullable=True),
        sa.Column("location", sa.Text(), nullable=True),
        sa.Column("coord_x", sa.Float(), nullable=True),
        sa.Column("coord_y", sa.Float(), nullable=True),
        sa.Column("is_hard_hit", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("is_sweet_spot", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("estimated_ba_using_speedangle", sa.Float(), nullable=True),
        sa.Column("estimated_woba_using_speedangle", sa.Float(), nullable=True),
        sa.Column("launch_speed_angle", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["batter_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["game_pk"], ["mlb_games.game_pk"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["pitcher_id"], ["mlb_players.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("game_pk", "play_id", name="uq_mlb_batted_ball_event_play"),
    )
    op.create_index(
        "ix_mlb_batted_ball_events_game_inning",
        "mlb_batted_ball_events",
        ["game_pk", "inning", "half_inning"],
    )
    op.create_index(
        "ix_mlb_batted_ball_events_pitcher_batter",
        "mlb_batted_ball_events",
        ["pitcher_id", "batter_id"],
    )

    op.create_table(
        "mlb_statcast_batter_season",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("season", sa.Integer(), nullable=False),
        sa.Column("player_id", sa.Integer(), nullable=False),
        sa.Column("player_name", sa.Text(), nullable=False),
        sa.Column("team_abbreviation", sa.Text(), nullable=True),
        sa.Column("plate_appearances", sa.Integer(), nullable=True),
        sa.Column("at_bats", sa.Integer(), nullable=True),
        sa.Column("hits", sa.Integer(), nullable=True),
        sa.Column("home_runs", sa.Integer(), nullable=True),
        sa.Column("strikeouts", sa.Integer(), nullable=True),
        sa.Column("walks", sa.Integer(), nullable=True),
        sa.Column("avg", sa.Float(), nullable=True),
        sa.Column("obp", sa.Float(), nullable=True),
        sa.Column("slg", sa.Float(), nullable=True),
        sa.Column("ops", sa.Float(), nullable=True),
        sa.Column("iso", sa.Float(), nullable=True),
        sa.Column("babip", sa.Float(), nullable=True),
        sa.Column("xba", sa.Float(), nullable=True),
        sa.Column("xslg", sa.Float(), nullable=True),
        sa.Column("xwoba", sa.Float(), nullable=True),
        sa.Column("xobp", sa.Float(), nullable=True),
        sa.Column("xiso", sa.Float(), nullable=True),
        sa.Column("exit_velocity_avg", sa.Float(), nullable=True),
        sa.Column("launch_angle_avg", sa.Float(), nullable=True),
        sa.Column("barrel_batted_rate", sa.Float(), nullable=True),
        sa.Column("hard_hit_percent", sa.Float(), nullable=True),
        sa.Column("sweet_spot_percent", sa.Float(), nullable=True),
        sa.Column("source_pull_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["player_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["source_pull_id"], ["mlb_source_pulls.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("season", "player_id", name="uq_mlb_statcast_batter_season"),
    )
    op.create_index("ix_mlb_statcast_batter_season_season", "mlb_statcast_batter_season", ["season"])

    op.create_table(
        "mlb_statcast_pitcher_season",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("season", sa.Integer(), nullable=False),
        sa.Column("player_id", sa.Integer(), nullable=False),
        sa.Column("player_name", sa.Text(), nullable=False),
        sa.Column("team_abbreviation", sa.Text(), nullable=True),
        sa.Column("batters_faced", sa.Integer(), nullable=True),
        sa.Column("strikeout_percent", sa.Float(), nullable=True),
        sa.Column("walk_percent", sa.Float(), nullable=True),
        sa.Column("xera", sa.Float(), nullable=True),
        sa.Column("xba", sa.Float(), nullable=True),
        sa.Column("xslg", sa.Float(), nullable=True),
        sa.Column("xwoba", sa.Float(), nullable=True),
        sa.Column("exit_velocity_avg", sa.Float(), nullable=True),
        sa.Column("launch_angle_avg", sa.Float(), nullable=True),
        sa.Column("barrel_batted_rate", sa.Float(), nullable=True),
        sa.Column("hard_hit_percent", sa.Float(), nullable=True),
        sa.Column("source_pull_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["player_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["source_pull_id"], ["mlb_source_pulls.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("season", "player_id", name="uq_mlb_statcast_pitcher_season"),
    )
    op.create_index("ix_mlb_statcast_pitcher_season_season", "mlb_statcast_pitcher_season", ["season"])

    op.create_table(
        "mlb_bat_tracking_batter_season",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("season", sa.Integer(), nullable=False),
        sa.Column("player_id", sa.Integer(), nullable=False),
        sa.Column("player_name", sa.Text(), nullable=False),
        sa.Column("team_abbreviation", sa.Text(), nullable=True),
        sa.Column("bat_speed", sa.Float(), nullable=True),
        sa.Column("fast_swing_rate", sa.Float(), nullable=True),
        sa.Column("squared_up_rate", sa.Float(), nullable=True),
        sa.Column("blast_rate", sa.Float(), nullable=True),
        sa.Column("blasts", sa.Float(), nullable=True),
        sa.Column("swing_length", sa.Float(), nullable=True),
        sa.Column("source_pull_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["player_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["source_pull_id"], ["mlb_source_pulls.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("season", "player_id", name="uq_mlb_bat_tracking_batter_season"),
    )
    op.create_index("ix_mlb_bat_tracking_batter_season_season", "mlb_bat_tracking_batter_season", ["season"])

    op.create_table(
        "mlb_swing_path_batter_season",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("season", sa.Integer(), nullable=False),
        sa.Column("player_id", sa.Integer(), nullable=False),
        sa.Column("player_name", sa.Text(), nullable=False),
        sa.Column("team_abbreviation", sa.Text(), nullable=True),
        sa.Column("attack_angle", sa.Float(), nullable=True),
        sa.Column("attack_direction", sa.Float(), nullable=True),
        sa.Column("ideal_attack_angle_rate", sa.Float(), nullable=True),
        sa.Column("swing_path_tilt", sa.Float(), nullable=True),
        sa.Column("source_pull_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["player_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["source_pull_id"], ["mlb_source_pulls.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("season", "player_id", name="uq_mlb_swing_path_batter_season"),
    )
    op.create_index("ix_mlb_swing_path_batter_season_season", "mlb_swing_path_batter_season", ["season"])

    op.create_table(
        "mlb_park_factors",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("season", sa.Integer(), nullable=False),
        sa.Column("venue_id", sa.Integer(), nullable=True),
        sa.Column("venue_name", sa.Text(), nullable=False),
        sa.Column("factor_type", sa.Text(), nullable=False),
        sa.Column("stat_key", sa.Text(), nullable=False),
        sa.Column("stat_value", sa.Float(), nullable=True),
        sa.Column("bat_side", sa.Text(), nullable=True),
        sa.Column("condition", sa.Text(), nullable=True),
        sa.Column("rolling_value", sa.Text(), nullable=True),
        sa.Column("tracking", sa.Text(), nullable=True),
        sa.Column("speed_bucket", sa.Text(), nullable=True),
        sa.Column("angle_bucket", sa.Text(), nullable=True),
        sa.Column("temperature_factor", sa.Float(), nullable=True),
        sa.Column("elevation_factor", sa.Float(), nullable=True),
        sa.Column("roof_factor", sa.Float(), nullable=True),
        sa.Column("environment_factor", sa.Float(), nullable=True),
        sa.Column("source_pull_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["source_pull_id"], ["mlb_source_pulls.id"]),
        sa.ForeignKeyConstraint(["venue_id"], ["mlb_venues.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "season",
            "venue_name",
            "factor_type",
            "stat_key",
            "bat_side",
            "condition",
            "rolling_value",
            "tracking",
            "speed_bucket",
            "angle_bucket",
            name="uq_mlb_park_factor_lookup",
        ),
    )
    op.create_index("ix_mlb_park_factors_season_stat", "mlb_park_factors", ["season", "stat_key"])
    op.create_index("ix_mlb_park_factors_venue", "mlb_park_factors", ["venue_name"])


def downgrade() -> None:
    op.drop_index("ix_mlb_park_factors_venue", table_name="mlb_park_factors")
    op.drop_index("ix_mlb_park_factors_season_stat", table_name="mlb_park_factors")
    op.drop_table("mlb_park_factors")
    op.drop_index("ix_mlb_swing_path_batter_season_season", table_name="mlb_swing_path_batter_season")
    op.drop_table("mlb_swing_path_batter_season")
    op.drop_index("ix_mlb_bat_tracking_batter_season_season", table_name="mlb_bat_tracking_batter_season")
    op.drop_table("mlb_bat_tracking_batter_season")
    op.drop_index("ix_mlb_statcast_pitcher_season_season", table_name="mlb_statcast_pitcher_season")
    op.drop_table("mlb_statcast_pitcher_season")
    op.drop_index("ix_mlb_statcast_batter_season_season", table_name="mlb_statcast_batter_season")
    op.drop_table("mlb_statcast_batter_season")
    op.drop_index("ix_mlb_batted_ball_events_pitcher_batter", table_name="mlb_batted_ball_events")
    op.drop_index("ix_mlb_batted_ball_events_game_inning", table_name="mlb_batted_ball_events")
    op.drop_table("mlb_batted_ball_events")
    op.drop_index("ix_mlb_pitch_events_pitcher_batter", table_name="mlb_pitch_events")
    op.drop_index("ix_mlb_pitch_events_game_inning", table_name="mlb_pitch_events")
    op.drop_table("mlb_pitch_events")
    op.drop_index("ix_mlb_player_game_pitching_game_team", table_name="mlb_player_game_pitching")
    op.drop_table("mlb_player_game_pitching")
    op.drop_index("ix_mlb_player_game_batting_game_team", table_name="mlb_player_game_batting")
    op.drop_table("mlb_player_game_batting")
    op.drop_index("ix_mlb_lineup_snapshots_game_team_order", table_name="mlb_lineup_snapshots")
    op.drop_table("mlb_lineup_snapshots")
    op.drop_index("ix_mlb_game_snapshots_game_type_captured", table_name="mlb_game_snapshots")
    op.drop_table("mlb_game_snapshots")
    op.drop_index("ix_mlb_games_season", table_name="mlb_games")
    op.drop_index("ix_mlb_games_official_date", table_name="mlb_games")
    op.drop_table("mlb_games")
    op.drop_table("mlb_players")
    op.drop_table("mlb_teams")
    op.drop_table("mlb_venues")
    op.drop_index("ix_mlb_source_pulls_season", table_name="mlb_source_pulls")
    op.drop_index("ix_mlb_source_pulls_game_pk", table_name="mlb_source_pulls")
    op.drop_index("ix_mlb_source_pulls_source_resource", table_name="mlb_source_pulls")
    op.drop_table("mlb_source_pulls")
