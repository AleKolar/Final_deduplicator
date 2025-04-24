import asyncio
import httpx
import orjson
from aio_pika import connect_robust
from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError as PydanticValidationError

from my_venv.src.config import settings
from my_venv.src.database.database import AsyncSessionLocal, get_redis
from my_venv.src.models.pydentic_models import EventCreate
from my_venv.src.services.deduplicator import Deduplicator
from my_venv.src.services.repository import PostgresEventRepository
from my_venv.src.utils.exceptions import DuplicateEventError
from my_venv.src.utils.logger import logger


async def handle_event_message(message: AbstractIncomingMessage, redis):
    """Обработчик для событий из основной очереди"""
    try:
        async with message.process():
            # Десериализация и валидация данных
            raw_data = orjson.loads(message.body)
            event = EventCreate(**raw_data)

            async with AsyncSessionLocal() as session:
                deduplicator = Deduplicator(redis)
                repo = PostgresEventRepository(session, deduplicator)

                # Сохранение валидированных данных
                created_event = await repo.create_event(event.model_dump())
                logger.info(f"Event processed: {created_event.event_hash}")

            await message.ack()

    except PydanticValidationError as e:
        logger.error(f"Invalid event data: {e.errors()}")
        await message.nack(requeue=False)
    except DuplicateEventError as e:
        logger.warning(f"Duplicate event: {e}")
        await message.ack()
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}", exc_info=True)
        await message.nack(requeue=False)


async def handle_sync_message(message: AbstractIncomingMessage, redis):
    """Обработчик для синхронизации внешних событий"""
    try:
        async with message.process():
            async with AsyncSessionLocal() as session:
                deduplicator = Deduplicator(redis)
                repo = PostgresEventRepository(session, deduplicator)

                await _fetch_external_events(repo)

            await message.ack()

    except Exception as e:
        logger.error(f"Sync failed: {str(e)}", exc_info=True)
        await message.nack(requeue=False)


async def _fetch_external_events(repo: PostgresEventRepository):
    """Загрузка и валидация событий из внешнего API"""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                settings.EXTERNAL_EVENTS_URL,
                headers={"Authorization": f"Bearer {settings.EXTERNAL_API_KEY}"}
            )
            response.raise_for_status()

            for event_data in response.json():
                try:
                    # Валидация через Pydantic модель
                    event = EventCreate(**event_data)
                    await repo.create_event(event.model_dump())
                except (PydanticValidationError, DuplicateEventError) as e:
                    logger.warning(f"Skipping invalid event: {str(e)}")
                    continue

    except httpx.HTTPError as e:
        logger.error(f"External API error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected sync error: {str(e)}")


async def main():
    """Основная функция запуска воркера с обработкой shutdown"""
    connection = None
    redis = None

    try:
        # Инициализация подключений
        redis = await get_redis()
        connection = await connect_robust(
            settings.RABBITMQ_URL,
            client_properties={"connection_name": "event_worker"}
        )

        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=10)

            # Объявление очередей
            events_queue = await channel.declare_queue(
                "events_queue",
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "dead_letter",
                    "x-dead-letter-routing-key": "dead_letter"
                }
            )

            sync_queue = await channel.declare_queue(
                "external_sync_queue",
                durable=True
            )

            # Подписка на очереди с передачей redis
            await events_queue.consume(lambda msg: handle_event_message(msg, redis))
            await sync_queue.consume(lambda msg: handle_sync_message(msg, redis))

            logger.info("Worker started. Waiting for messages...")
            await asyncio.Future()

    except asyncio.CancelledError:
        logger.info("Worker shutdown requested")
    except Exception as e:
        logger.critical(f"Critical worker failure: {str(e)}")
    finally:
        # Корректное закрытие подключений
        if connection:
            await connection.close()
        if redis:
            await redis.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")