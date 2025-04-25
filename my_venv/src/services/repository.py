from typing import Dict, Any, List, Optional
from datetime import datetime

from sqlalchemy import select, and_, func, between, or_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from my_venv.src.models.ORM_models import EventIncomingORM as Event
from my_venv.src.models.pydentic_models import EventResponse
from my_venv.src.services.deduplicator import Deduplicator
from my_venv.src.utils.exceptions import DatabaseError
from my_venv.src.utils.logger import logger


async def is_duplicate(redis, event_hash):
    return Deduplicator.is_duplicate(event_hash, redis)


class PostgresEventRepository:
    def __init__(self, session: AsyncSession, deduplicator: Deduplicator):
        self.session = session
        self.deduplicator = deduplicator

    async def is_duplicate(self, event_hash: str) -> bool:
        stmt = select(Event.id).where(Event.event_hash == event_hash).limit(1)
        result = await self.session.execute(stmt)
        return bool(result.scalar())

    async def create_event(self, event_data: dict) -> tuple[Optional[Event], bool]:
        try:
            this_is_duplicate = await self.is_duplicate(event_data["event_hash"])
            if this_is_duplicate:
                return None, True

            event = Event(**event_data)
            self.session.add(event)
            await self.session.commit()
            await self.session.refresh(event)
            return event, False

        except SQLAlchemyError as e:
            await self.session.rollback()
            raise DatabaseError("Failed to save event") from e

    async def get_events(
            self,
            filters: Dict[str, Any],
            limit: int = 100,
            offset: int = 0,
            event_name: Optional[str] = None,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None
    ) -> List[EventResponse]:
        """Получение событий с фильтрацией"""
        try:
            query = select(Event)

            if event_name:
                query = query.where(
                    Event.raw_data["event_name"].as_string() == event_name
                )

            if start_date or end_date:
                conditions = []
                if start_date:
                    conditions.append(Event.created_at >= start_date)
                if end_date:
                    conditions.append(Event.created_at <= end_date)
                query = query.where(and_(*conditions))

            for field, value in filters.items():
                query = query.where(
                    Event.raw_data[field].as_string() == str(value)
                )

            result = await self.session.execute(query.offset(offset).limit(limit))
            return [EventResponse.model_config(e) for e in result.scalars().all()]

        except SQLAlchemyError as e:
            logger.error(f"Ошибка запроса: {str(e)}")
            raise DatabaseError("Ошибка получения событий")

    async def get_events_count(
            self,
            filters: Optional[Dict[str, Any]] = None,
            time_range: Optional[Dict[str, datetime]] = None
    ) -> int:
        """Получение общего количества событий"""
        try:
            query = select(func.count(Event.id))

            if time_range:
                query = query.where(
                    between(
                        Event.created_at,
                        time_range.get("start"),
                        time_range.get("end")
                    )
                )
            if filters:
                for field, value in filters.items():
                    query = query.where(
                        Event.raw_data[field].as_string() == str(value)
                    )

            result = await self.session.execute(query)
            return result.scalar_one()

        except SQLAlchemyError as e:
            logger.error(f"Ошибка подсчета: {str(e)}")
            raise DatabaseError("Ошибка подсчета событий")

    async def search_events(
            self,
            query: str,
            fields: List[str],
            limit: int = 100
    ) -> Dict[str, Any]:
        """Поиск событий по тексту"""
        try:
            conditions = [Event.raw_data[field].as_string().ilike(f"%{query}%")
                          for field in fields]

            result = await self.session.execute(
                select(Event)
                .where(or_(*conditions))
                .limit(limit)
            )

            return {
                "results": [
                    EventResponse.model_config(e)
                    for e in result.scalars().all()
                ]
            }

        except SQLAlchemyError as e:
            logger.error(f"Ошибка поиска: {str(e)}")
            raise DatabaseError("Ошибка выполнения поиска")
