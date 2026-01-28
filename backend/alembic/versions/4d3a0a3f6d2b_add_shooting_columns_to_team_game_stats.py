"""add shooting columns to team_game_stats

Revision ID: 4d3a0a3f6d2b
Revises: 2b6c0f4a6a3e
Create Date: 2026-01-28 10:35:00.000000
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "4d3a0a3f6d2b"
down_revision = "2b6c0f4a6a3e"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("team_game_stats", sa.Column("fgm", sa.Float()))
    op.add_column("team_game_stats", sa.Column("fga", sa.Float()))
    op.add_column("team_game_stats", sa.Column("fg3m", sa.Float()))
    op.add_column("team_game_stats", sa.Column("fg3a", sa.Float()))


def downgrade():
    op.drop_column("team_game_stats", "fg3a")
    op.drop_column("team_game_stats", "fg3m")
    op.drop_column("team_game_stats", "fga")
    op.drop_column("team_game_stats", "fgm")
