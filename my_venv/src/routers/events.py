from datetime import datetime
from typing import Dict, Any, List, Optional

from aio_pika.abc import AbstractRobustConnection
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession
import httpx
import aio_pika
import orjson
from redis.asyncio import Redis

from my_venv.src.config import settings
from my_venv.src.database.database import get_db
from my_venv.src.models.pydentic_models import EventCreate, EventResponse
from my_venv.src.services.deduplicator import Deduplicator
from my_venv.src.services.repository import EventRepository, PostgresEventRepository
from my_venv.src.utils.exceptions import (
    DatabaseError,
    DuplicateEventError,
    ExternalServerError,
    MessageQueueError
)

router = APIRouter(prefix="/events", tags=["events"])


# region Dependency Injections
async def get_redis() -> Redis:
    """Возвращает асинхронное подключение к Redis"""
    redis = Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=5
    )
    try:
        yield redis
    finally:
        await redis.close()


async def get_rabbitmq() -> AbstractRobustConnection:
    """Устанавливает и возвращает подключение к RabbitMQ"""
    try:
        return await aio_pika.connect_robust(
            settings.RABBITMQ_URL,
            timeout=10
        )
    except Exception as e:
        raise MessageQueueError(f"Ошибка подключения к RabbitMQ: {str(e)}")


async def get_repository(
        session: AsyncSession = Depends(get_db),
        redis: Redis = Depends(get_redis)
) -> EventRepository:
    # Создаем Deduplicator из Redis подключения
    deduplicator = Deduplicator(redis)

    # Возвращаем КОНКРЕТНУЮ реализацию для PostgreSQL
    return PostgresEventRepository(
        session=session,
        deduplicator=deduplicator
    )


# endregion

# region Event Endpoints
@router.post("/", response_model=EventResponse, status_code=201)
async def create_event(
        payload: Dict[str, Any],
        rabbitmq: aio_pika.Connection = Depends(get_rabbitmq)
):
    """
    Создание нового события через брокер сообщений
    """
    try:
        # Валидация и подготовка данных
        event = EventCreate(**payload)
        response_data = EventResponse(**event.model_dump())

        # Публикация в очередь
        async with rabbitmq.channel() as channel:
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=orjson.dumps(event.model_dump()),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    headers={"source": "api"}
                ),
                routing_key="events_queue"
            )

        return response_data

    except ValidationError as e:
        raise HTTPException(422, detail=e.errors())
    except MessageQueueError as e:
        raise HTTPException(503, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail="Internal Server Error")


@router.get("/", response_model=List[EventResponse])
async def get_events(
        page: int = Query(1, ge=1, description="Номер страницы"),
        per_page: int = Query(100, ge=1, le=1000, description="Элементов на странице"),
        event_name: Optional[str] = Query(None),
        start_date: Optional[datetime] = Query(None),
        end_date: Optional[datetime] = Query(None),
        repo: EventRepository = Depends(get_repository)):
    """Получение событий с пагинацией и фильтрами"""
    try:
        offset = (page - 1) * per_page

        # Загрузка внешних событий
        await _fetch_external_events(repo, page, per_page)

        # Получение данных из БД
        events = await repo.get_events(
            limit=per_page,
            offset=offset,
            event_name=event_name,
            start_date=start_date,
            end_date=end_date
        )

        return [EventResponse.model_validate(e.to_response()) for e in events]

    except DatabaseError as e:
        raise HTTPException(400, detail=str(e))
    except ExternalServerError as e:
        raise HTTPException(502, detail=f"Ошибка внешнего сервера: {str(e)}")
    except Exception:
        raise HTTPException(500, detail="Internal Server Error")


@router.get("/search", response_model=Dict[str, Any])
async def search_events(
        query: str = Query(..., min_length=3, description="Поисковый запрос"),
        page: int = Query(1, ge=1, description="Номер страницы"),
        per_page: int = Query(100, ge=1, le=1000, description="Элементов на странице"),
        continuation_token: Optional[str] = Query(None),
        repo: EventRepository = Depends(get_repository)
):
    """Поиск событий с курсорной пагинацией"""
    try:
        # Синхронизация с внешним источником
        await _fetch_external_events(repo, page, per_page)

        # Выполнение поиска
        result = await repo.search_events(
            query=query,
            page=page,
            per_page=per_page,
            continuation_token=continuation_token
        )

        return {
            "results": [EventResponse.model_validate(e.to_response()) for e in result["results"]],
            "continuation_token": result.get("continuation_token")
        }

    except DatabaseError as e:
        raise HTTPException(400, detail=str(e))
    except ExternalServerError as e:
        raise HTTPException(502, detail=f"Ошибка внешнего сервера: {str(e)}")
    except Exception:
        raise HTTPException(500, detail="Internal Server Error")


# endregion

# region Helper Functions
async def _fetch_external_events(
        repo: EventRepository,
        page: int,
        per_page: int
):
    """Загрузка событий из внешнего источника"""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                settings.EXTERNAL_EVENTS_URL,
                json={"page": page, "per_page": per_page},
                headers={"Authorization": f"Bearer {settings.EXTERNAL_API_KEY}"}
            )
            response.raise_for_status()

            for event_data in response.json():
                try:
                    validated = EventCreate(**event_data).model_dump()
                    await repo.create_event(validated)
                except (ValidationError, DuplicateEventError) as e:
                    continue

    except httpx.HTTPError as e:
        raise ExternalServerError(f"Ошибка API: {str(e)}")
    except Exception as e:
        raise ExternalServerError(f"Неизвестная ошибка: {str(e)}")
# endregion