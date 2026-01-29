"""add player_under_risk

Revision ID: 9b2c3f4d5e6f
Revises: 8c1065ec23b5
Create Date: 2026-01-29 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "9b2c3f4d5e6f"
down_revision: Union[str, Sequence[str], None] = "8c1065ec23b5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "player_under_risk",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("player_id", sa.Integer(), nullable=True),
        sa.Column("stat_type", sa.Text(), nullable=False),
        sa.Column("window_n", sa.Integer(), nullable=False),
        sa.Column("sample_size", sa.Integer(), nullable=False),
        sa.Column("under_count", sa.Integer(), nullable=False),
        sa.Column("under_rate", sa.Float(), nullable=False),
        sa.Column("threshold_type", sa.Text(), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=True),
        sa.Column("computed_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["player_id"], ["players.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("player_id", "stat_type", name="uq_player_under_risk"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("player_under_risk")
