from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings
from app.db.url_utils import to_async_db_url

ml_engine = create_async_engine(
    to_async_db_url(settings.ML_DATABASE_URL),
    echo=True,
    pool_pre_ping=True,
    pool_recycle=300,
)

MlbAsyncSessionLocal = async_sessionmaker(
    bind=ml_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_mlb_db():
    async with MlbAsyncSessionLocal() as session:
        yield session
