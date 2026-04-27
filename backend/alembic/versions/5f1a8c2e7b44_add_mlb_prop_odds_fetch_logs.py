"""add mlb prop odds fetch logs

Revision ID: 5f1a8c2e7b44
Revises: 2e4b7c9a1d30
Create Date: 2026-04-27 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "5f1a8c2e7b44"
down_revision: Union[str, Sequence[str], None] = "2e4b7c9a1d30"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "mlb_prop_odds_fetch_logs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("sport", sa.Text(), nullable=False),
        sa.Column("market", sa.Text(), nullable=False),
        sa.Column("bookmaker", sa.Text(), nullable=False),
        sa.Column("game_date", sa.Date(), nullable=False),
        sa.Column("status", sa.Text(), server_default="completed", nullable=False),
        sa.Column("props_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("events_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "provider",
            "bookmaker",
            "market",
            "game_date",
            name="uq_mlb_prop_odds_fetch_log_lookup",
        ),
    )
    op.create_index(
        "ix_mlb_prop_odds_fetch_logs_lookup",
        "mlb_prop_odds_fetch_logs",
        ["provider", "bookmaker", "market", "game_date", "fetched_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_mlb_prop_odds_fetch_logs_lookup", table_name="mlb_prop_odds_fetch_logs")
    op.drop_table("mlb_prop_odds_fetch_logs")
