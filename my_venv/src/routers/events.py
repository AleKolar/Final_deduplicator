from datetime import datetime
from typing import Dict, Any, List, Optional

import orjson
from aio_pika.abc import AbstractRobustConnection
from fastapi import APIRouter, Depends, HTTPException, Query, status, Body
from fastapi.encoders import jsonable_encoder
from pydantic import ValidationError
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import aio_pika
from aio_pika import Message, DeliveryMode

from my_venv.src.config import settings
from my_venv.src.database.database import get_db, get_redis
from my_venv.src.models.pydentic_models import EventCreate, EventResponse
from my_venv.src.services.deduplicator import Deduplicator
from my_venv.src.services.repository import PostgresEventRepository
from my_venv.src.utils.exceptions import DatabaseError, MessageQueueError
from my_venv.src.utils.helper import remove_empty_values
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
    return PostgresEventRepository(session=session, deduplicator=Deduplicator(redis))


@router.post("/", response_model=EventResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_event(
        payload: Dict[str, Any],
        rabbitmq: aio_pika.Connection = Depends(get_rabbitmq)
):
    """Обработка входящего события"""
    try:
        # 1. Очистка данных от пустых значений
        cleaned_data = remove_empty_values(payload)

        # 2. Валидация и создание объекта события
        event = EventCreate(**cleaned_data)

        # 3. Подготовка данных для отправки
        event_data = event.model_dump()

        # 4. Публикация в RabbitMQ
        async with rabbitmq.channel() as channel:
            await channel.default_exchange.publish(
                Message(
                    body=orjson.dumps(event_data),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    headers={"source": "api"}
                ),
                routing_key="events_queue"
            )

        # 5. Возврат ответа
        return EventResponse(
            **event_data,
            id="queued",
            created_at=datetime.now()
        )

    except ValidationError as e:
        errors = jsonable_encoder(e.errors())
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"errors": errors}
        )

    except MessageQueueError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Ошибка подключения к брокеру сообщений"
        )

    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
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

@router.get("/sort", response_model=List[EventResponse])
async def get_events(
    page: int = Query(1, ge=1, description="Номер страницы"),
    per_page: int = Query(100, ge=1, le=1000, description="Количество элементов на странице"),
    event_name: Optional[str] = Query(None, description="Фильтр по имени события"),
    start_date: Optional[datetime] = Query(None, description="Начальная дата фильтрации"),
    filters: Optional[Dict[str, Any]] = None,
    end_date: Optional[datetime] = Query(None, description="Конечная дата фильтрации"),
    repo: PostgresEventRepository = Depends(get_repository)
) -> List[EventResponse]:
    """Получение обработанных событий с пагинацией"""
    try:
        offset = (page - 1) * per_page
        filters = filters or {}
        return await repo.get_events(
            limit=per_page,
            offset=offset,
            event_name=event_name,
            start_date=start_date,
            end_date=end_date,
            filters=filters
        )

    except DatabaseError as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception:
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )


@router.get("/health")
async def health_check(
        db: AsyncSession = Depends(get_db),
        redis: Redis = Depends(get_redis),
        rabbitmq: aio_pika.Connection = Depends(get_rabbitmq)  # Исправлено на использование зависимости
):
    """Проверка состояния сервиса"""
    try:
        # Проверка БД
        await db.execute(text("SELECT 1"))

        # Проверка Redis
        await redis.ping()

        # Проверка RabbitMQ
        async with rabbitmq.channel() as channel:
            await channel.declare_queue("health_check", auto_delete=True)

        return {"status": "OK"}

    except Exception as e:
        logger.error("Health check failed: %s", str(e))
        raise HTTPException(503, detail=str(e))


@router.post("/endpoint")
async def your_endpoint(data: dict = Body(...)):
    print(data)
    return {"message": "Data received"}