import aio_pika
import orjson
import asyncio
from aio_pika.abc import (
    AbstractConnection,
    AbstractChannel,
    AbstractIncomingMessage,
    AbstractQueue,
)

from my_venv.src.config import settings
from my_venv.src.database.database import AsyncSessionLocal, engine
from my_venv.src.models.ORM_models import EventIncomingORM
from my_venv.src.utils.logger import logger


# Добавим валидацию входных данных
def validate_event_data(event_data: dict) -> bool:
    required_fields = {"event_hash", "event_name", "event_datetime"}
    return all(field in event_data for field in required_fields)


async def process_message(event_data: dict):
    try:
        if not validate_event_data(event_data):
            raise ValueError("Invalid event data structure")

        async with AsyncSessionLocal() as session:
            event_orm = EventIncomingORM(**event_data)
            session.add(event_orm)
            await session.commit()
            logger.info(f"Event saved: {event_data['event_hash']}")

    except Exception as e:
        logger.error(f"DB Error: {str(e)}")
        await session.rollback()
        raise


async def handle_message(message: AbstractIncomingMessage):
    session = None
    try:
        async with message.process():
            event_data = orjson.loads(message.body)
            session = AsyncSessionLocal()

            event_orm = EventIncomingORM(**event_data)
            session.add(event_orm)
            await session.commit()
            await message.ack()

            logger.info(f"Event saved: {event_data['event_hash']}")

    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        if session:
            await session.rollback()
        await message.nack(requeue=False)
    finally:
        if session:
            await session.close()

async def consume_events():
    connection: AbstractConnection | None = None
    try:
        # Подключение с таймаутом
        connection = await asyncio.wait_for(
            aio_pika.connect(settings.RABBITMQ_URL),
            timeout=10.0
        )

        channel: AbstractChannel = await connection.channel()
        await channel.set_qos(prefetch_count=100)

        # Объявление очереди с дополнительными параметрами
        queue: AbstractQueue = await channel.declare_queue(
            name="events",
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": "dead_letters"
            }
        )

        logger.info("Worker started. Waiting for messages...")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                await handle_message(message)

    except asyncio.TimeoutError:
        logger.error("Connection to RabbitMQ timed out")
    except asyncio.CancelledError:
        logger.info("Worker stopping gracefully...")
    except Exception as e:
        logger.critical(f"Critical error: {str(e)}", exc_info=True)
        raise
    finally:
        if connection:
            await connection.close()
        await engine.dispose()


if __name__ == "__main__":
    try:
        asyncio.run(consume_events())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    finally:
        logger.info("Worker process terminated")