"""add mlb roster snapshots

Revision ID: 6e2c9b8a4f12
Revises: 4c2b6d91e3fa
Create Date: 2026-04-26 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "6e2c9b8a4f12"
down_revision: Union[str, Sequence[str], None] = "4c2b6d91e3fa"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "mlb_roster_snapshots",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("team_id", sa.Integer(), nullable=False),
        sa.Column("player_id", sa.Integer(), nullable=False),
        sa.Column("roster_type", sa.Text(), nullable=False),
        sa.Column("roster_date", sa.Date(), nullable=False),
        sa.Column("season", sa.Integer(), nullable=True),
        sa.Column("jersey_number", sa.Text(), nullable=True),
        sa.Column("status_code", sa.Text(), nullable=True),
        sa.Column("status_description", sa.Text(), nullable=True),
        sa.Column("position_code", sa.Text(), nullable=True),
        sa.Column("position_name", sa.Text(), nullable=True),
        sa.Column("position_type", sa.Text(), nullable=True),
        sa.Column("position_abbreviation", sa.Text(), nullable=True),
        sa.Column("is_pitcher", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("source_pull_id", sa.Integer(), nullable=True),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["player_id"], ["mlb_players.id"]),
        sa.ForeignKeyConstraint(["source_pull_id"], ["mlb_source_pulls.id"]),
        sa.ForeignKeyConstraint(["team_id"], ["mlb_teams.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "team_id",
            "player_id",
            "roster_type",
            "roster_date",
            name="uq_mlb_roster_snapshot_player",
        ),
    )
    op.create_index(
        "ix_mlb_roster_snapshots_team_date",
        "mlb_roster_snapshots",
        ["team_id", "roster_date", "roster_type"],
    )
    op.create_index(
        "ix_mlb_roster_snapshots_pitchers",
        "mlb_roster_snapshots",
        ["roster_date", "team_id", "is_pitcher"],
    )


def downgrade() -> None:
    op.drop_index("ix_mlb_roster_snapshots_pitchers", table_name="mlb_roster_snapshots")
    op.drop_index("ix_mlb_roster_snapshots_team_date", table_name="mlb_roster_snapshots")
    op.drop_table("mlb_roster_snapshots")
