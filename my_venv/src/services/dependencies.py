from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from my_venv.src.database.database import AsyncSessionLocal, REDIS_URL


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session

async def get_redis() -> Redis:
    return await Redis.from_url(REDIS_URL)