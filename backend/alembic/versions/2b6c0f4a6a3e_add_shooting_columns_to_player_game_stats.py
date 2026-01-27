"""add shooting columns to player_game_stats

Revision ID: 2b6c0f4a6a3e
Revises: 7a74b57c08ca
Create Date: 2026-01-28 10:05:00.000000
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "2b6c0f4a6a3e"
down_revision = "7a74b57c08ca"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("player_game_stats", sa.Column("fgm", sa.Float()))
    op.add_column("player_game_stats", sa.Column("fga", sa.Float()))
    op.add_column("player_game_stats", sa.Column("fg3m", sa.Float()))
    op.add_column("player_game_stats", sa.Column("fg3a", sa.Float()))


def downgrade():
    op.drop_column("player_game_stats", "fg3a")
    op.drop_column("player_game_stats", "fg3m")
    op.drop_column("player_game_stats", "fga")
    op.drop_column("player_game_stats", "fgm")
