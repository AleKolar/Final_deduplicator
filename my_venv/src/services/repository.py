import json
from datetime import timedelta, datetime
from typing import Dict, Any, Optional, Tuple, List
from aio_pika.abc import AbstractRobustConnection
from redis.asyncio import Redis
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from my_venv.src.models.ORM_models import EventIncomingORM as Event
from my_venv.src.models.pydentic_models import EventResponse
from my_venv.src.utils.exceptions import DatabaseError
from my_venv.src.utils.logger import logger
from my_venv.src.utils.serializer import JsonSerializer


class EventRepository:
    def __init__(
        self,
        db_session: AsyncSession,
        redis: Redis,
        rabbitmq: Optional[AbstractRobustConnection] = None
    ):
        self.db = db_session
        self.redis = redis
        self.rabbitmq = rabbitmq
        self.unique_counter = 0
        self._rabbitmq_channel = None

    async def _get_rabbitmq_channel(self):
        """Ленивая инициализация канала RabbitMQ"""
        if not self._rabbitmq_channel and self.rabbitmq:
            self._rabbitmq_channel = await self.rabbitmq.channel()
            await self._rabbitmq_channel.declare_queue("events_queue")
        return self._rabbitmq_channel

    async def _check_redis_duplicate(self, event_hash: str) -> bool:
        return await self.redis.exists(event_hash)

    async def _check_postgres_duplicate(self, event_hash: str) -> bool:
        result = await self.db.execute(
            select(Event.id).where(Event.event_hash == event_hash).limit(1)
        )
        return bool(result.scalar())

    async def is_duplicate(self, event_hash: str) -> Tuple[bool, str]:
        redis_dup = await self._check_redis_duplicate(event_hash)
        if redis_dup:
            return True, "duplicate_in_redis"

        pg_dup = await self._check_postgres_duplicate(event_hash)
        if pg_dup:
            return True, "duplicate_in_postgres"

        return False, "unique"

    async def save_event(self, event_data: dict) -> tuple:
        try:
            # Сохранение в PostgreSQL
            event = Event(**event_data)
            self.db.add(event)
            await self.db.commit()
            await self.db.refresh(event)

            # Сохранение в Redis
            await self.redis.setex(
                name=event_data['event_hash'],
                time=int(timedelta(days=7).total_seconds()),
                value="1"
            )

            self.unique_counter += 1
            return event, "saved"
        except Exception:
            await self.db.rollback()
            await self.redis.delete(event_data.get('event_hash', ''))
            raise

    async def get_stats(self) -> Dict[str, int]:
        return {
            "unique_in_session": self.unique_counter,
            "total_in_redis": await self.redis.dbsize(),
            "total_in_postgres": await self._get_pg_count()
        }

    async def _get_pg_count(self) -> int:
        result = await self.db.execute(select(func.count(Event.id)))
        return result.scalar_one()

    async def get_events(
        self,
        limit: int = 100,
        offset: int = 0,
        event_name: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[EventResponse]:
        try:
            query = select(Event)

            if event_name:
                query = query.where(Event.event_name == event_name)

            date_filters = []
            if start_date:
                date_filters.append(Event.created_at >= start_date)
            if end_date:
                date_filters.append(Event.created_at <= end_date)
            if date_filters:
                query = query.where(and_(*date_filters))

            if filters:
                for field, value in filters.items():
                    query = query.where(
                        Event.raw_data[field].as_string() == str(value)
                    )

            query = query.offset(offset).limit(limit)
            result = await self.db.execute(query)
            events = result.scalars().all()

            return [
                EventResponse.model_validate(
                    json.loads(JsonSerializer.serialize(event))
                ) for event in events
            ]

        except Exception as e:
            logger.error(f"Error retrieving events: {str(e)}", exc_info=True)
            raise DatabaseError("Failed to retrieve events")