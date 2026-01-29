"""merge heads

Revision ID: 2abd7d80ba59
Revises: 4d3a0a3f6d2b, 9b2c3f4d5e6f
Create Date: 2026-01-29 19:59:33.127388

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2abd7d80ba59'
down_revision: Union[str, Sequence[str], None] = ('4d3a0a3f6d2b', '9b2c3f4d5e6f')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
