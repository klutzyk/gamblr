"""add mlb prop odds snapshots

Revision ID: 2e4b7c9a1d30
Revises: 9d7f3a2c8b10
Create Date: 2026-04-27 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "2e4b7c9a1d30"
down_revision: Union[str, Sequence[str], None] = "9d7f3a2c8b10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "mlb_prop_odds_snapshots",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("sport", sa.Text(), nullable=False),
        sa.Column("market", sa.Text(), nullable=False),
        sa.Column("bookmaker", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("game_date", sa.Date(), nullable=False),
        sa.Column("commence_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("home_team", sa.Text(), nullable=True),
        sa.Column("away_team", sa.Text(), nullable=True),
        sa.Column("player_name", sa.Text(), nullable=False),
        sa.Column("normalized_player_name", sa.Text(), nullable=False),
        sa.Column("line", sa.Float(), nullable=True),
        sa.Column("american_odds", sa.Integer(), nullable=False),
        sa.Column("decimal_odds", sa.Float(), nullable=False),
        sa.Column("implied_probability", sa.Float(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "provider",
            "bookmaker",
            "market",
            "event_id",
            "normalized_player_name",
            "line",
            name="uq_mlb_prop_odds_snapshot_lookup",
        ),
    )
    op.create_index(
        "ix_mlb_prop_odds_snapshots_lookup",
        "mlb_prop_odds_snapshots",
        ["provider", "bookmaker", "market", "game_date"],
    )
    op.create_index(
        "ix_mlb_prop_odds_snapshots_fetched_at",
        "mlb_prop_odds_snapshots",
        ["fetched_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_mlb_prop_odds_snapshots_fetched_at", table_name="mlb_prop_odds_snapshots")
    op.drop_index("ix_mlb_prop_odds_snapshots_lookup", table_name="mlb_prop_odds_snapshots")
    op.drop_table("mlb_prop_odds_snapshots")
