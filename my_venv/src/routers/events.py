from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Body
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import aio_pika

from my_venv.src.database.database import get_db, get_redis
from my_venv.src.models.pydentic_models import EventResponse, EventCreate, EventBase
from my_venv.src.services.dependencies import get_repository, get_rabbitmq
from my_venv.src.services.event_hashing import EventHashService
from my_venv.src.services.repository import EventRepository
from my_venv.src.utils.logger import logger
from my_venv.src.utils.serializer import DataNormalizer

router = APIRouter(prefix="/events", tags=["events"])

# Регистрация зависимостей
# async def get_rabbitmq() -> AbstractRobustConnection:
#     """Устанавливает подключение к RabbitMQ"""
#     try:
#         connection = await aio_pika.connect_robust(
#             url=settings.RABBITMQ_URL,
#             timeout=10,
#             client_properties={"connection_name": "api_connection"}
#         )
#         return connection
#     except Exception as e:
#         raise MessageQueueError(
#             message="Ошибка подключения к брокеру сообщений",
#             error_details=str(e)
#         )

@router.post("/events")
async def process_events(events: List[EventCreate], repo: EventRepository = Depends(get_repository)):
    results = {"total_processed": 0, "saved": 0, "duplicates": 0, "errors": []}

    for event in events:
        results["total_processed"] += 1
        try:
            raw_data = event.model_dump()
            logger.debug(f"Raw event data: {raw_data}")

            event_hash = EventHashService.generate_unique_fingerprint(
                raw_data,
                raw_data.get('event_name', 'default_event')
            )
            logger.debug(f"Generated hash: {event_hash}")

            if await repo.is_duplicate(event_hash):
                logger.info(f"Duplicate detected: {event_hash}")
                results["duplicates"] += 1
                continue

            cleaned_data = DataNormalizer.deep_clean(raw_data)
            logger.debug(f"Cleaned data: {cleaned_data}")

            cleaned_data['event_hash'] = event_hash
            await repo.save_event(cleaned_data)
            results["saved"] += 1

        except Exception as e:
            logger.error(f"Error processing event: {str(e)}", exc_info=True)
            results["errors"].append({"error": str(e)})

    return results



@router.get("/events/stats", response_model=Dict[str, int])
async def get_system_stats(
        repo: EventRepository = Depends(get_repository)
) -> Dict[str, int]:
    return await repo.get_stats()


@router.get("/events", response_model=List[EventResponse])  #
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
        rabbitmq: aio_pika.Connection = Depends(get_rabbitmq)
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