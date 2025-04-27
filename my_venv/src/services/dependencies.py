from aio_pika.abc import AbstractRobustConnection
from fastapi import Depends
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from my_venv.src.database.database import AsyncSessionLocal, REDIS_URL, get_db, get_redis

from my_venv.src.routers.events import get_rabbitmq
from my_venv.src.services.repository import EventRepository

#
# async def get_db() -> AsyncSession:
#     async with AsyncSessionLocal() as session:
#         yield session
#
# async def get_redis() -> Redis:
#     return await Redis.from_url(REDIS_URL)

async def get_repository(
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
    rabbitmq: AbstractRobustConnection = Depends(get_rabbitmq)
) -> EventRepository:
    return EventRepository(
        db_session=db,
        redis=redis,  # !!! Redis-клиент
        rabbitmq=rabbitmq
    )