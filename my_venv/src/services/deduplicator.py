from typing import Callable

from redis.asyncio import Redis

from my_venv.src.services.event_hashing import EventHashService


class DeduplicationError(Exception):
    pass

class Deduplicator:
    def __init__(self, redis: Redis):
        self.redis = redis

    async def check_and_lock(self, event_hash: str) -> bool:
        """
        Атомарная проверка и блокировка события.
        Возвращает True, если событие уникально (не найдено в Redis).
        """
        try:
            return await self.redis.set(
                name=event_hash,
                value=1,
                nx=True,
                ex=604800  # TTL 7 дней
            )
        except Exception as e:
            raise DeduplicationError(f"Redis operation failed: {str(e)}")

    async def safe_save(self, event_hash: str, save_callback: Callable):
        """
        Безопасное сохранение с откатом при ошибках
        """
        try:
            if await self.check_and_lock(event_hash):
                await save_callback()
        except Exception as e:
            # Откатываем блокировку при ошибке
            await self.redis.delete(event_hash)
            raise DeduplicationError(f"Rollback due to error: {str(e)}")

