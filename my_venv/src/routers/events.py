from datetime import datetime
from typing import List, Optional
from aio_pika.abc import AbstractRobustConnection
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import ValidationError
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import aio_pika
from aio_pika import Message, DeliveryMode
import orjson

from my_venv.src.config import settings
from my_venv.src.database.database import get_db, get_redis
from my_venv.src.models.ORM_models import Event
from my_venv.src.models.pydentic_models import EventCreate, EventResponse
from my_venv.src.services.deduplicator import Deduplicator
from my_venv.src.services.repository import PostgresEventRepository
from my_venv.src.utils.exceptions import MessageQueueError, DatabaseError
from my_venv.src.utils.logger import logger

router = APIRouter(prefix="/events", tags=["events"])

# Регистрация зависимостей
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

async def get_repository(
    session: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis)
) -> PostgresEventRepository:
    """Фабрика репозитория с зависимостями"""
    return PostgresEventRepository(
        session=session,
        deduplicator=Deduplicator(redis)
    )


@router.post("/", response_model=EventResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_event(
        payload: dict,
        rabbitmq: AbstractRobustConnection = Depends(get_rabbitmq),
        repo: PostgresEventRepository = Depends(get_repository)
):
    try:
        logger.debug(f"Received payload: {payload}")

        # 1. Предобработка данных
        processed_data = {}
        for key, value in payload.items():
            if isinstance(value, bytes):
                logger.warning(f"Bytes detected in field {key}, converting to string")
                processed_data[key] = value.decode('utf-8', errors='replace')
            else:
                processed_data[key] = value

        # 2. Валидация данных
        try:
            event = EventCreate(**processed_data)
        except ValidationError as e:
            logger.error(f"Validation error: {e.errors()}")
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={"errors": e.errors()}
            )

        # 3. Сохранение в БД
        try:
            db_event, is_duplicate = await repo.create_event(event.model_dump())
        except DatabaseError as e:
            logger.error(f"Database error: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Ошибка сохранения события"
            )

        # 4. Отправка в RabbitMQ
        try:
            async with rabbitmq.channel() as channel:
                message = Message(
                    body=orjson.dumps(event.model_dump()),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    headers={"source": "api"}
                )
                await channel.default_exchange.publish(
                    message=message,
                    routing_key="events_queue"
                )
                logger.debug("Message successfully published to RabbitMQ")

        except aio_pika.exceptions.AMQPError as e:
            logger.error(f"RabbitMQ publish error: {str(e)}")
            raise MessageQueueError("Failed to publish message to queue")

        # 5. Формирование ответа
        return EventResponse(
            **event.model_dump(),
            id=db_event.id if db_event else None,
            created_at=db_event.created_at if db_event else datetime.now(),
            system_timestamps={
                "received_at": datetime.now(),
                "processed_at": db_event.processed_at if db_event else None
            }
        )

    except MessageQueueError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e)
        )

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@router.post("/sync-external", status_code=status.HTTP_202_ACCEPTED)
async def trigger_external_sync(
    rabbitmq: aio_pika.Connection = Depends(get_rabbitmq)
):
    """Запуск синхронизации с внешним источником"""
    try:
        async with rabbitmq.channel() as channel:
            await channel.default_exchange.publish(
                Message(
                    body=b'',  # Пустое сообщение для триггера
                    headers={"task_type": "external_sync"},
                    delivery_mode=DeliveryMode.PERSISTENT
                ),
                routing_key="external_sync_queue"
            )

        return {"status": "Синхронизация поставлена в очередь"}

    except Exception:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Не удалось запланировать синхронизацию"
        )


@router.get("/", response_model=List[EventResponse])
async def get_events(
        self,
        limit: int = 100,
        offset: int = 0,
        event_name: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
) -> List[EventResponse]:
    query = select(Event)

    if event_name:
        query = query.where(Event.raw_data["event_name"].astext == event_name)

    if start_date and end_date:
        query = query.where(Event.created_at.between(start_date, end_date))
    elif start_date:
        query = query.where(Event.created_at >= start_date)
    elif end_date:
        query = query.where(Event.created_at <= end_date)

    query = query.limit(limit).offset(offset)

    result = await self.session.execute(query)
    return [EventResponse.from_orm(e) for e in result.scalars()]

# # Статистика
# @router.get("/stats", response_model=dict)
# async def get_event_stats(repo: PostgresEventRepository = Depends(get_repository)):
#     return {
#         "unique_events": repo.unique_count,
#         "duplicate_events": repo.duplicate_count
#     }

