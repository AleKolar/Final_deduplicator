import aio_pika
from aio_pika.abc import AbstractRobustConnection
from fastapi import Depends
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from my_venv.src.config import settings
from my_venv.src.database.database import get_db, get_redis
from my_venv.src.services.deduplicator import Deduplicator
from my_venv.src.services.repository import EventRepository
from my_venv.src.utils.exceptions import MessageQueueError


async def get_rabbitmq() -> AbstractRobustConnection:
    """Устанавливает подключение к RabbitMQ"""
    try:
        connection = await aio_pika.connect_robust(
            url=settings.RABBITMQ_URL,
            timeout=10,
            client_properties={"connection_name": "api_connection"}
        )
        return connection
    except Exception as e:
        raise MessageQueueError(
            message="Ошибка подключения к брокеру сообщений",
            error_details=str(e)
        )

# async def get_db() -> AsyncSession:
#     async with AsyncSessionLocal() as session:
#         yield session
#
# async def get_redis() -> Redis:
#     return await Redis.from_url(
#         settings.REDIS_URL,
#         decode_responses=True,
#         socket_connect_timeout=5
#     )


def get_deduplicator(redis: Redis = Depends(get_redis)) -> Deduplicator:
    return Deduplicator(redis=redis)

async def get_repository(
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
    rabbitmq: AbstractRobustConnection = Depends(get_rabbitmq)
) -> EventRepository:
    return EventRepository(
        db_session=db,
        redis=redis,
        rabbitmq=rabbitmq
    )