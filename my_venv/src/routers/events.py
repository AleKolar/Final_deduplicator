from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import ValidationError, model_validator
from sqlalchemy.ext.asyncio import AsyncSession
import httpx
import aio_pika
import orjson
from redis.asyncio import Redis
from fastapi.encoders import jsonable_encoder

from my_venv.src.config import settings
from my_venv.src.database.database import get_db
from my_venv.src.models.pydentic_models import EventCreate, EventResponse
from my_venv.src.services.deduplicator import Deduplicator
from my_venv.src.services.repository import PostgresEventRepository
from my_venv.src.services.event_hashing import EventHashService, HashGenerationError
from my_venv.src.utils.exceptions import (
    DatabaseError,
    DuplicateEventError,
    ExternalServerError,
    MessageQueueError
)

router = APIRouter(prefix="/events", tags=["events"])


# region Helper Functions
def remove_empty_values(data: Dict[str, Any]) -> Dict[str, Any]:
    """Рекурсивно удаляет пустые значения из словаря"""

    def is_empty(value):
        if isinstance(value, (list, dict)):
            return not bool(value)
        if isinstance(value, str):
            return not value.strip()
        return value in (None, "", {}, [])

    if isinstance(data, dict):
        return {
            k: remove_empty_values(v)
            for k, v in data.items()
            if not is_empty(v)
        }
    return data


def is_invalid_event(event_data: Dict[str, Any]) -> bool:
    """Проверяет наличие критически пустых полей"""
    required_fields = {'event_name', 'event_datetime', 'user_agent'}
    return any(
        not event_data.get(field)
        for field in required_fields
    )


# endregion

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


async def get_rabbitmq() -> aio_pika.Connection:
    """Устанавливает подключение к RabbitMQ"""
    try:
        return await aio_pika.connect_robust(
            settings.RABBITMQ_URL,
            timeout=10
        )
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


# endregion

# region Event Endpoints
@router.post("/", response_model=EventResponse, status_code=201)
async def create_event(
        payload: Dict[str, Any],
        rabbitmq: aio_pika.Connection = Depends(get_rabbitmq)
):
    """Создание нового события через брокер сообщений"""
    try:
        # Очистка и проверка данных
        cleaned_data = remove_empty_values(payload)

        if is_invalid_event(cleaned_data):
            raise HTTPException(400, detail="Event has missing required fields")

        # Валидация и подготовка данных
        event = EventCreate(**cleaned_data)
        event_data = event.model_dump()

        # Генерация хэша
        event_hash = EventHashService.generate(
            raw_data=event_data,
            event_name=event.event_name,
            use_timestamp=False
        )
        event_data['event_hash'] = event_hash

        # Публикация в очередь
        async with rabbitmq.channel() as channel:
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=orjson.dumps(event_data),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    headers={"source": "api"}
                ),
                routing_key="events_queue"
            )

        return EventResponse(
            **event_data,
            id=0,  # Временное значение
            created_at=datetime.utcnow()
        )

    except ValidationError as e:
        errors = jsonable_encoder(e.errors(), custom_encoder={datetime: lambda v: v.isoformat()})
        raise HTTPException(422, detail=errors)
    except HashGenerationError as e:
        raise HTTPException(400, detail=str(e))
    except MessageQueueError as e:
        raise HTTPException(503, detail=str(e))
    except Exception:
        raise HTTPException(500, detail="Internal Server Error")


@router.get("/", response_model=List[EventResponse])
async def get_events(
        page: int = Query(1, ge=1),
        per_page: int = Query(100, ge=1, le=1000),
        event_name: Optional[str] = Query(None),
        start_date: Optional[datetime] = Query(None),
        end_date: Optional[datetime] = Query(None),
        repo: PostgresEventRepository = Depends(get_repository)
):
    """Получение событий с пагинацией и фильтрами"""
    try:
        offset = (page - 1) * per_page
        await _fetch_external_events(repo, page, per_page)

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
        query: str = Query(..., min_length=3),
        page: int = Query(1, ge=1),
        per_page: int = Query(100, ge=1, le=1000),
        continuation_token: Optional[str] = Query(None),
        repo: PostgresEventRepository = Depends(get_repository)
):
    """Поиск событий с курсорной пагинацией"""
    try:
        await _fetch_external_events(repo, page, per_page)

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

# region External Events Processing
async def _fetch_external_events(
        repo: PostgresEventRepository,
        page: int,
        per_page: int
):
    """Загрузка и обработка событий из внешнего источника"""
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
                    cleaned_data = remove_empty_values(event_data)
                    if is_invalid_event(cleaned_data):
                        continue

                    validated = EventCreate(**cleaned_data).model_dump()
                    await repo.create_event(validated)
                except (ValidationError, DuplicateEventError):
                    continue

    except httpx.HTTPError as e:
        raise ExternalServerError(f"Ошибка API: {str(e)}")
    except Exception as e:
        raise ExternalServerError(f"Неизвестная ошибка: {str(e)}")
# endregion