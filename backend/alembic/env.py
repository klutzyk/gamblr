from logging.config import fileConfig
import asyncio
import os
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import context
from app.core.config import settings
from app.db.base import Base
from app.db.url_utils import to_async_db_url, to_sync_db_url
from app.models import (
    event,
    bookmaker,
    market,
    player_prop,
    player,
    player_game_stat,
    game_schedule,
    team,
    team_game_stat,
    lineup_stat,
    first_basket_label,
    first_basket_prediction_log,
    ingestion_run,
    mlb,
)


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

database_url = os.getenv("ALEMBIC_DATABASE_URL") or settings.DATABASE_URL
config.set_main_option("sqlalchemy.url", to_sync_db_url(database_url))

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (generates SQL scripts)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    """Helper for async migration"""
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode with async engine."""

    async def async_run():
        # Alembic expects a sync connection, so we use run_sync
        engine = create_async_engine(to_async_db_url(database_url), poolclass=pool.NullPool)
        async with engine.begin() as conn:
            await conn.run_sync(do_run_migrations)
        await engine.dispose()

    asyncio.run(async_run())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
