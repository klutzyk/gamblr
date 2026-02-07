"""add ingestion_runs

Revision ID: cc3f1c2d7a01
Revises: b1f6e2c4a9d1
Create Date: 2026-02-06 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "cc3f1c2d7a01"
down_revision: Union[str, Sequence[str], None] = "b1f6e2c4a9d1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "ingestion_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("ingest_type", sa.Text(), nullable=False),
        sa.Column("since_date", sa.Date(), nullable=True),
        sa.Column("season", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("players_total", sa.Integer(), nullable=False),
        sa.Column("players_saved", sa.Integer(), nullable=False),
        sa.Column("players_skipped", sa.Integer(), nullable=False),
        sa.Column("players_failed", sa.Integer(), nullable=False),
        sa.Column("total_new_games_inserted", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ingestion_runs_type_since",
        "ingestion_runs",
        ["ingest_type", "since_date"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_ingestion_runs_type_since", table_name="ingestion_runs")
    op.drop_table("ingestion_runs")
