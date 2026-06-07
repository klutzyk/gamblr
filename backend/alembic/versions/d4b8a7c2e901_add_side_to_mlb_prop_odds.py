"""add side to mlb prop odds

Revision ID: d4b8a7c2e901
Revises: a8f4c2d9b731
Create Date: 2026-06-07 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d4b8a7c2e901"
down_revision: Union[str, Sequence[str], None] = "a8f4c2d9b731"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "mlb_prop_odds_snapshots",
        sa.Column("side", sa.Text(), server_default="Over", nullable=False),
    )
    op.drop_constraint(
        "uq_mlb_prop_odds_snapshot_lookup",
        "mlb_prop_odds_snapshots",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_mlb_prop_odds_snapshot_lookup",
        "mlb_prop_odds_snapshots",
        [
            "provider",
            "bookmaker",
            "market",
            "event_id",
            "normalized_player_name",
            "side",
            "line",
        ],
    )
    op.alter_column("mlb_prop_odds_snapshots", "side", server_default=None)


def downgrade() -> None:
    op.drop_constraint(
        "uq_mlb_prop_odds_snapshot_lookup",
        "mlb_prop_odds_snapshots",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_mlb_prop_odds_snapshot_lookup",
        "mlb_prop_odds_snapshots",
        [
            "provider",
            "bookmaker",
            "market",
            "event_id",
            "normalized_player_name",
            "line",
        ],
    )
    op.drop_column("mlb_prop_odds_snapshots", "side")
