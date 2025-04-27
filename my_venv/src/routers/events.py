from datetime import datetime
from typing import Dict, Any, List, Optional
from aio_pika.abc import AbstractRobustConnection
from fastapi import APIRouter, Depends, HTTPException, Query, status, Body
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import aio_pika

from my_venv.src.config import settings
from my_venv.src.database.database import get_db, get_redis
from my_venv.src.models.pydentic_models import EventResponse
from my_venv.src.services.repository import EventRepository
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
) -> EventRepository:
    """Фабрика репозитория с зависимостями"""
    return EventRepository(redis=redis, db_session=session)


@router.post(
    "/events",
    response_model=Dict[str, Any],
    status_code=status.HTTP_207_MULTI_STATUS
)
async def process_events_batch(
        events: List[Dict[str, Any]],
        repo: EventRepository = Depends(get_repository)  # Используем готовую зависимость
) -> Dict[str, Any]:
    results = {
        "total_processed": 0,
        "saved": 0,
        "duplicates": 0,
        "errors": [],
        "session_stats": None
    }

    for event in events:
        try:
            results["total_processed"] += 1
            _, status = await repo.save_event(event)

            if "duplicate" in status:
                results["duplicates"] += 1
            else:
                results["saved"] += 1

        except Exception as e:
            results["errors"].append({
                "event_data": event,
                "error": str(e)
            })

    stats = await repo.get_stats()
    results["session_stats"] = {
        "unique_in_session": repo.unique_counter,
        "total_in_system": stats["total_in_postgres"] + stats["total_in_redis"]
    }
    return results


@router.get("/sort", response_model=List[EventResponse])
async def get_events(
    page: int = Query(1, ge=1, description="Номер страницы"),
    per_page: int = Query(100, ge=1, le=1000, description="Количество элементов на странице"),
    event_name: Optional[str] = Query(None, description="Фильтр по имени события"),
    start_date: Optional[datetime] = Query(None, description="Начальная дата фильтрации"),
    filters: Optional[Dict[str, Any]] = None,
    end_date: Optional[datetime] = Query(None, description="Конечная дата фильтрации"),
    repo: EventRepository = Depends(get_repository)
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
async def endpoint(data: dict = Body(...)):
    print(data)
    return {"message": "Data received"}