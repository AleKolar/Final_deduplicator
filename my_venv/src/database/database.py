import redis
from redis.asyncio import from_url, Redis
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from my_venv.src.config import settings
from my_venv.src.models.ORM_models import Base

DATABASE_URL = settings.DATABASE_URL
REDIS_URL = settings.REDIS_URL

engine = create_async_engine(
    settings.DATABASE_URL,
    pool_size=20,
    max_overflow=10,
    pool_timeout=30,
    echo=False
)


AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session

async def create_tables():
   async with engine.begin() as conn:
       await conn.run_sync(Base.metadata.create_all)

async def get_redis() -> Redis:
    my_redis = Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_keepalive=True
    )
    return my_redis
