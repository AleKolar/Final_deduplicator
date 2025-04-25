from redis.asyncio import Redis

class Deduplicator:
    def __init__(self, redis: Redis):
        self.redis = redis

    # Проверяет, существует ли данный ключ в Redis
    async def is_duplicate(self, key: str) -> int:
        return await self.redis.exists(key)

    # Устанавливает ключ в Redis с временем жизни 7 дней
    async def mark_processed(self, key: str):
         await self.redis.setex(key, 604800, 1)

    # Проверяет, существует ли уникальный ключ, если нет - устанавливает его
    async def check_and_lock(self, key: str) -> bool:
        """ Атомарно проверяет уникальность и блокирует ключ на 7 дней """
        return await self.redis.set(key, 1, nx=True, ex=604800)

