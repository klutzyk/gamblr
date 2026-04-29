"""add mlb weather and umpire tables

Revision ID: 4c2b6d91e3fa
Revises: 1f0d3f5b9c21
Create Date: 2026-04-24 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "4c2b6d91e3fa"
down_revision: Union[str, Sequence[str], None] = "1f0d3f5b9c21"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "mlb_umpires",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("full_name", sa.Text(), nullable=False),
        sa.Column("first_name", sa.Text(), nullable=True),
        sa.Column("last_name", sa.Text(), nullable=True),
        sa.Column("jersey_number", sa.Text(), nullable=True),
        sa.Column("job", sa.Text(), nullable=True),
        sa.Column("job_id", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "mlb_game_official_assignments",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("snapshot_id", sa.Integer(), nullable=False),
        sa.Column("game_pk", sa.BigInteger(), nullable=False),
        sa.Column("umpire_id", sa.Integer(), nullable=False),
        sa.Column("official_type", sa.Text(), nullable=False),
        sa.Column("is_home_plate", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.ForeignKeyConstraint(["game_pk"], ["mlb_games.game_pk"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["snapshot_id"], ["mlb_game_snapshots.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["umpire_id"], ["mlb_umpires.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "snapshot_id",
            "umpire_id",
            "official_type",
            name="uq_mlb_game_official_assignment",
        ),
    )
    op.create_index(
        "ix_mlb_game_official_assignments_game_snapshot",
        "mlb_game_official_assignments",
        ["game_pk", "snapshot_id"],
    )
    op.create_index(
        "ix_mlb_game_official_assignments_umpire",
        "mlb_game_official_assignments",
        ["umpire_id"],
    )

    op.create_table(
        "mlb_weather_snapshots",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("game_pk", sa.BigInteger(), nullable=False),
        sa.Column("venue_id", sa.Integer(), nullable=True),
        sa.Column("source_pull_id", sa.Integer(), nullable=True),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("dataset", sa.Text(), nullable=False),
        sa.Column("pulled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("target_time_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("game_time_offset_hours", sa.Float(), nullable=True),
        sa.Column("temperature_2m_c", sa.Float(), nullable=True),
        sa.Column("relative_humidity_2m", sa.Float(), nullable=True),
        sa.Column("dew_point_2m_c", sa.Float(), nullable=True),
        sa.Column("surface_pressure_hpa", sa.Float(), nullable=True),
        sa.Column("pressure_msl_hpa", sa.Float(), nullable=True),
        sa.Column("wind_speed_10m_kph", sa.Float(), nullable=True),
        sa.Column("wind_direction_10m_deg", sa.Float(), nullable=True),
        sa.Column("wind_gusts_10m_kph", sa.Float(), nullable=True),
        sa.Column("cloud_cover_percent", sa.Float(), nullable=True),
        sa.Column("visibility_m", sa.Float(), nullable=True),
        sa.Column("precipitation_probability", sa.Float(), nullable=True),
        sa.Column("precipitation_mm", sa.Float(), nullable=True),
        sa.Column("rain_mm", sa.Float(), nullable=True),
        sa.Column("showers_mm", sa.Float(), nullable=True),
        sa.Column("snowfall_cm", sa.Float(), nullable=True),
        sa.Column("weather_code", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["game_pk"], ["mlb_games.game_pk"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["source_pull_id"], ["mlb_source_pulls.id"]),
        sa.ForeignKeyConstraint(["venue_id"], ["mlb_venues.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "game_pk",
            "provider",
            "dataset",
            "pulled_at",
            "target_time_utc",
            name="uq_mlb_weather_snapshot_lookup",
        ),
    )
    op.create_index(
        "ix_mlb_weather_snapshots_game_target",
        "mlb_weather_snapshots",
        ["game_pk", "target_time_utc"],
    )
    op.create_index(
        "ix_mlb_weather_snapshots_game_pulled",
        "mlb_weather_snapshots",
        ["game_pk", "pulled_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_mlb_weather_snapshots_game_pulled", table_name="mlb_weather_snapshots")
    op.drop_index("ix_mlb_weather_snapshots_game_target", table_name="mlb_weather_snapshots")
    op.drop_table("mlb_weather_snapshots")

    op.drop_index("ix_mlb_game_official_assignments_umpire", table_name="mlb_game_official_assignments")
    op.drop_index(
        "ix_mlb_game_official_assignments_game_snapshot",
        table_name="mlb_game_official_assignments",
    )
    op.drop_table("mlb_game_official_assignments")

    op.drop_table("mlb_umpires")
