from datetime import datetime
from typing import Dict, Any, List, Optional

import orjson
from aio_pika.abc import AbstractRobustConnection
from fastapi import APIRouter, Depends, HTTPException, Query, status, Body, Request
from fastapi.encoders import jsonable_encoder
from pydantic import ValidationError
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import aio_pika
from aio_pika import Message, DeliveryMode
from starlette.responses import JSONResponse

from my_venv.src.config import settings
from my_venv.src.database.database import get_db, get_redis
from my_venv.src.models.pydentic_models import EventCreate, EventResponse, EventRequest
from my_venv.src.services.deduplicator import Deduplicator
from my_venv.src.services.normalize import preprocess_input, remove_empty_values
from my_venv.src.services.repository import PostgresEventRepository
from my_venv.src.utils.exceptions import DatabaseError, MessageQueueError

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
        payload: dict = Body(...),
        rabbitmq: aio_pika.Connection = Depends(get_rabbitmq),
):
    """Обработка входящего события с улучшенной обработкой ошибок"""
    logger.debug(f"Received event: {payload}")
    try:
        processed_data = preprocess_input(payload)
        data_without_id = {k: v for k, v in processed_data.items() if k != 'id'}
        # 1. Валидация playtime_ms с учетом None
        playtime = processed_data.get('playtime_ms')
        if playtime is not None and not isinstance(playtime, (int, float)):
            raise ValueError("playtime_ms must be numeric or null")
        # 2. Проверка обязательных полей
        # if "client_id" not in processed_data:
        #     logger.error("Missing client_id in processed data")
        #     raise HTTPException(
        #         status_code=status.HTTP_400_BAD_REQUEST,
        #         detail="Missing required field: client_id"
        #     )

        # 3. Валидация с помощью Pydantic
        try:
            event = EventCreate.model_validate(data_without_id)
            # event = EventCreate(**processed_data)
        except ValidationError as e:
            logger.error(f"Validation error: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=jsonable_encoder(e.errors())
            )

        # 4. Подготовка данных для отправки
        event_data = event.model_dump(exclude={'created_at'})

        # 5. Отправка в RabbitMQ
        try:
            async with rabbitmq.channel() as channel:
                await channel.default_exchange.publish(
                    Message(
                        body=orjson.dumps(event_data),
                        delivery_mode=DeliveryMode.PERSISTENT,
                        headers={"source": "api"}
                    ),
                    routing_key="events_queue"
                )
        except Exception as e:
            logger.error(f"RabbitMQ error: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Message broker unavailable"
            )

        # 6. Формирование ответа
        return EventResponse(
            **event_data,
            created_at=datetime.now(),
        )

    except HTTPException:
        # Уже обработанные ошибки
        raise

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
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

@router.post("/events/")
async def handle_event(event_data: EventRequest):
    try:
        processed_data = preprocess_input(event_data.model_dump())
        if "client_id" not in processed_data:
            raise ValueError("Missing client_id")
        return {"status": "success", "data": processed_data}
    except Exception as e:
        logger.error(f"Processing error: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=400,
            content={"detail": "Ошибка обработки данных", "error": str(e)}
        )


@router.post("/endpoint")
async def endpoint(data: dict = Body(...)):
    print(data)
    return {"message": "Data received"}