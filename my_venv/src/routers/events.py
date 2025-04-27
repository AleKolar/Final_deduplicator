import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from aio_pika.abc import AbstractRobustConnection
from fastapi import APIRouter, Depends, HTTPException, status, Body
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import aio_pika

from my_venv.src.config import settings
from my_venv.src.database.database import get_db, get_redis
from my_venv.src.models.pydentic_models import EventResponse, EventCreate
from my_venv.src.services.event_hashing import EventHashService
from my_venv.src.services.repository import EventRepository
from my_venv.src.utils.exceptions import MessageQueueError

from my_venv.src.utils.logger import logger
from my_venv.src.utils.serializer import JSONRepairEngine

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
async def process_events(
        events: List[EventCreate],
        repo: EventRepository = Depends(get_repository)
) -> Dict[str, Any]:
    results = {
        "total_processed": 0,
        "saved": 0,
        "duplicates": 0,
        "errors": [],
        "session_stats": None
    }

    for event_data in events:
        results["total_processed"] += 1
        try:
            # Конвертация в dict и обработка experiments
            event_dict = event_data.model_dump()
            experiments = event_dict.get("experiments", "[]")

            # Исправление JSON
            fixed_experiments = JSONRepairEngine.fix_string(experiments)
            event_dict["experiments"] = json.loads(fixed_experiments)

            # Генерация хеша
            event_hash = EventHashService.generate_unique_fingerprint(
                event_dict,
                event_name=event_dict.get("event_type", "default")
            )

            # Проверка дубликатов
            is_dup, _ = await repo.is_duplicate(event_hash)
            if is_dup:
                results["duplicates"] += 1
                continue

            # Сохранение
            await repo.save_event({**event_dict, "event_hash": event_hash})
            results["saved"] += 1

        except Exception as e:
            results["errors"].append({
                "event_data": event_data.model_dump(),
                "error": str(e)
            })

    # Статистика
    stats = await repo.get_stats()
    results["session_stats"] = {
        "unique_in_session": repo.unique_counter,
        "total_in_system": stats["total_in_redis"] + stats["total_in_postgres"]
    }

    return results


@router.get("/events/stats", response_model=Dict[str, int])
async def get_system_stats(
        repo: EventRepository = Depends(get_repository)
) -> Dict[str, int]:
    return await repo.get_stats()


@router.get("/events", response_model=List[EventResponse])
async def search_events(
        event_name: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        repo: EventRepository = Depends(get_repository)
) -> List[EventResponse]:
    return await repo.get_events(
        limit=limit,
        offset=offset,
        event_name=event_name,
        start_date=start_date,
        end_date=end_date,
        filters=filters
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