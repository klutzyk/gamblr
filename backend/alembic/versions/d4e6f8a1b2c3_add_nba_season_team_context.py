"""add NBA season and historical team context

Revision ID: d4e6f8a1b2c3
Revises: a8f4c2d9b731
"""

from alembic import op
import sqlalchemy as sa


revision = "d4e6f8a1b2c3"
down_revision = "a8f4c2d9b731"
branch_labels = None
depends_on = None


def _season_backfill(table: str) -> None:
    op.execute(
        sa.text(
            f"""
            UPDATE {table}
            SET season = (
                CASE WHEN EXTRACT(MONTH FROM game_date) >= 7
                     THEN EXTRACT(YEAR FROM game_date)::integer
                     ELSE EXTRACT(YEAR FROM game_date)::integer - 1
                END
            )::text || '-' || LPAD((
                (CASE WHEN EXTRACT(MONTH FROM game_date) >= 7
                      THEN EXTRACT(YEAR FROM game_date)::integer
                      ELSE EXTRACT(YEAR FROM game_date)::integer - 1
                 END + 1) % 100
            )::text, 2, '0')
            WHERE season IS NULL
            """
        )
    )


def upgrade() -> None:
    op.add_column("player_game_stats", sa.Column("season", sa.Text(), nullable=True))
    op.add_column(
        "player_game_stats", sa.Column("team_abbreviation", sa.Text(), nullable=True)
    )
    op.add_column("team_game_stats", sa.Column("season", sa.Text(), nullable=True))
    _season_backfill("player_game_stats")
    _season_backfill("team_game_stats")
    op.execute(
        """
        UPDATE player_game_stats
        SET team_abbreviation = UPPER(SPLIT_PART(matchup, ' ', 1))
        WHERE team_abbreviation IS NULL AND matchup IS NOT NULL AND matchup <> ''
        """
    )
    op.create_index("ix_player_game_stats_season", "player_game_stats", ["season"])
    op.create_index("ix_team_game_stats_season", "team_game_stats", ["season"])


def downgrade() -> None:
    op.drop_index("ix_team_game_stats_season", table_name="team_game_stats")
    op.drop_index("ix_player_game_stats_season", table_name="player_game_stats")
    op.drop_column("team_game_stats", "season")
    op.drop_column("player_game_stats", "team_abbreviation")
    op.drop_column("player_game_stats", "season")
