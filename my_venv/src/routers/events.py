import json
from datetime import datetime
from typing import Any, List, Optional
from aio_pika.abc import AbstractRobustConnection
from fastapi import APIRouter, Depends, HTTPException, Query, status
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession
import aio_pika
from aio_pika import Message, DeliveryMode
import orjson

from my_venv.src.config import settings
from my_venv.src.database.database import get_db, get_redis
from my_venv.src.models.pydentic_models import EventCreate, EventResponse
from my_venv.src.services.deduplicator import Deduplicator
from my_venv.src.services.repository import PostgresEventRepository
from my_venv.src.utils.exceptions import DatabaseError, MessageQueueError
from my_venv.src.utils.helper import _preprocess_value
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
        rabbitmq: AbstractRobustConnection = Depends(get_rabbitmq)
):
    try:
        logger.debug(f"Raw input payload: {payload}")

        # Preprocess the payload, converting all bytes to strings as needed
        processed_payload = {k: _preprocess_value(v) for k, v in payload.items()}
        logger.debug(f"Processed payload: {processed_payload}")

        # Check for any unexpected values that might still be bytes
        for key, value in processed_payload.items():
            if isinstance(value, bytes):
                logger.error(f"Unexpected bytes found in payload key '{key}': {value}")
                raise ValueError(f"Unexpected type for key '{key}': bytes")

        event = EventCreate(**processed_payload)
        logger.debug(f"Processed event object: {event.model_dump()}")

        async with rabbitmq.channel() as channel:
            await channel.default_exchange.publish(
                Message(
                    body=orjson.dumps(event.model_dump()),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    headers={"source": "api"}
                ),
                routing_key="events_queue"
            )

        return EventResponse(
            **event.model_dump(),
            id="queued",
            created_at=datetime.now(),
            system_timestamps={
                "received_at": datetime.now(),
                "processed_at": datetime.now()
            }
        )
    except Exception as e:
        logger.error(f"Error processing the event: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/", response_model=List[EventResponse])
async def get_events(
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=1000),
    filters: Optional[str] = Query(None, description="JSON фильтры по raw_data"),
    time_field: Optional[str] = Query(None, description="Поле времени из timestamps"),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    repo: PostgresEventRepository = Depends(get_repository)
) -> List[EventResponse]:
    """Получение событий с динамической фильтрацией"""
    try:
        # Парсинг фильтров
        filter_dict = {}
        if filters:
            try:
                filter_dict = orjson.loads(filters)
            except orjson.JSONDecodeError:
                raise HTTPException(400, detail="Invalid filters format")

        # Формирование временного диапазона
        time_range = None
        if time_field or start_date or end_date:
            time_range = {
                "field": time_field or "created_at",
                "start": start_date,
                "end": end_date
            }

        return await repo.get_events(
            filters=filter_dict,
            time_range=time_range,
            limit=per_page,
            offset=(page-1)*per_page
        )

    except DatabaseError as e:
        raise HTTPException(400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(500, detail="Internal server error")

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


