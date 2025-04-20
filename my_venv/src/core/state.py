from typing import TypedDict
from aio_pika.abc import AbstractChannel
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine

class AppState(TypedDict):
    redis: Redis
    rabbit_channel: AbstractChannel
    db_engine: AsyncEngine