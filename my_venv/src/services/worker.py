import asyncio
import httpx
import orjson
from aio_pika import connect_robust
from aio_pika.abc import AbstractIncomingMessage
from pydantic_core import ValidationError
from redis.asyncio import Redis

from my_venv.src.config import settings
from my_venv.src.database.database import AsyncSessionLocal
from my_venv.src.services.deduplicator import Deduplicator
from my_venv.src.services.repository import PostgresEventRepository
from my_venv.src.utils.exceptions import DuplicateEventError
from my_venv.src.utils.logger import logger


async def handle_event_message(message: AbstractIncomingMessage):
    """Обработчик для событий из основной очереди"""
    try:
        async with message.process():
            event_data = orjson.loads(message.body)

            async with AsyncSessionLocal() as session:
                deduplicator = Deduplicator(Redis.from_url(settings.REDIS_URL))
                repo = PostgresEventRepository(session, deduplicator)

                created_event = await repo.create_event(event_data)
                logger.info(f"Event processed: {created_event.event_hash}")

            await message.ack()

    except DuplicateEventError as e:
        logger.warning(f"Duplicate event: {e.event_hash}")
        await message.ack()
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        await message.nack(requeue=False)


async def handle_sync_message(message: AbstractIncomingMessage):
    """Обработчик для синхронизации внешних событий"""
    try:
        async with message.process():
            async with AsyncSessionLocal() as session:
                deduplicator = Deduplicator(Redis.from_url(settings.REDIS_URL))
                repo = PostgresEventRepository(session, deduplicator)

                await _fetch_external_events(repo)

            await message.ack()

    except Exception as e:
        logger.error(f"Sync failed: {str(e)}")
        await message.nack(requeue=False)


async def _fetch_external_events(repo: PostgresEventRepository):
    """Загрузка событий из внешнего API"""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                settings.EXTERNAL_EVENTS_URL,
                headers={"Authorization": f"Bearer {settings.EXTERNAL_API_KEY}"}
            )
            response.raise_for_status()

            for event_data in response.json():
                try:
                    await repo.create_event(event_data)
                except (ValidationError, DuplicateEventError):
                    continue

    except httpx.HTTPError as e:
        logger.error(f"External API error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected sync error: {str(e)}")


async def main():
    """Основная функция запуска воркера"""
    connection = await connect_robust(
        settings.RABBITMQ_URL,
        client_properties={"connection_name": "event_worker"}
    )

    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=10)

        # Основная очередь событий
        events_queue = await channel.declare_queue(
            "events_queue",
            durable=True
        )

        # Очередь для синхронизации
        sync_queue = await channel.declare_queue(
            "external_sync_queue",
            durable=True
        )

        await events_queue.consume(handle_event_message)
        await sync_queue.consume(handle_sync_message)

        logger.info("Worker started. Waiting for messages...")
        await asyncio.Future()  # Бесконечное ожидание


if __name__ == "__main__":
    asyncio.run(main())