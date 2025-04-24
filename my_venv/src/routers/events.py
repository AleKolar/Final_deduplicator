from datetime import datetime
from typing import Dict, Any, List, Optional

from aio_pika.abc import AbstractRobustConnection
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.encoders import jsonable_encoder
from pydantic import ValidationError
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
from my_venv.src.utils.helper import remove_empty_values  # Предполагаем существование этого модуля

router = APIRouter(prefix="/events", tags=["events"])

# Регистрация зависимостей
async def get_rabbitmq() -> AbstractRobustConnection:
    """Устанавливает подключение к RabbitMQ"""
    try:
        return await aio_pika.connect_robust(
            settings.RABBITMQ_URL,
            timeout=10
        )
    except Exception as e:
        raise MessageQueueError(
            message="Ошибка подключения к брокеру сообщений",
            error_details=str(e))

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
        payload: Dict[str, Any],
        rabbitmq: aio_pika.Connection = Depends(get_rabbitmq)
):
    """Обработка входящего события"""
    try:
        # 1. Очистка данных от пустых значений
        cleaned_data = remove_empty_values(payload)

        # 2. Валидация и создание объекта события
        event = EventCreate(**cleaned_data)  # Может вызвать ValidationError

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
        # Сериализация ошибок валидации
        errors = jsonable_encoder(e.errors())  # Важно: e.errors() со скобками!
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
        # Общая ошибка сервера
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

@router.get("/", response_model=List[EventResponse])
async def get_events(
    page: int = Query(1, ge=1, description="Номер страницы"),
    per_page: int = Query(100, ge=1, le=1000, description="Количество элементов на странице"),
    event_name: Optional[str] = Query(None, description="Фильтр по имени события"),
    start_date: Optional[datetime] = Query(None, description="Начальная дата фильтрации"),
    end_date: Optional[datetime] = Query(None, description="Конечная дата фильтрации"),
    repo: PostgresEventRepository = Depends(get_repository)
) -> List[EventResponse]:
    """Получение обработанных событий с пагинацией"""
    try:
        offset = (page - 1) * per_page
        return await repo.get_events(
            limit=per_page,
            offset=offset,
            event_name=event_name,
            start_date=start_date,
            end_date=end_date
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