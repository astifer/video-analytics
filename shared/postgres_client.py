from sqlalchemy.ext.asyncio import create_async_engine


engine = create_async_engine(
    settings.db_url,
    pool_size=5,
    max_overflow=2,
    pool_pre_ping=True,
    pool_recycle=1800,
    echo=True,
)