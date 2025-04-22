# worker.py (модификация)
import orjson
from aio_pika.abc import AbstractIncomingMessage
from redis.asyncio import Redis

from my_venv.src.config import settings
from my_venv.src.database.database import AsyncSessionLocal
from my_venv.src.services.deduplicator import Deduplicator
from my_venv.src.services.repository import PostgresEventRepository
from my_venv.src.utils.exceptions import DuplicateEventError
from my_venv.src.utils.logger import logger


async def handle_message(message: AbstractIncomingMessage):
    repo = None
    try:
        async with message.process():
            event_data = orjson.loads(message.body)

            async with AsyncSessionLocal() as session:
                deduplicator = Deduplicator(Redis.from_url(settings.REDIS_URL))
                repo = PostgresEventRepository(session, deduplicator)

                created_event = await repo.create_event(event_data)
                logger.info(f"Event processed: {created_event.content_hash}")

            await message.ack()

    except DuplicateEventError:
        logger.warning(f"Duplicate event: {event_data.get('content_hash')}")
        await message.ack()
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        await message.nack(requeue=False)
    finally:
        if repo:
            await repo.session.close()