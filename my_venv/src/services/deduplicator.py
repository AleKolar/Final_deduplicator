from redis.asyncio import Redis

class Deduplicator:
    def __init__(self, redis: Redis):
        self.redis = redis

    async def is_duplicate(self, key: str) -> int:
        return await self.redis.exists(key)

    async def mark_processed(self, key: str):
        await self.redis.setex(key, 604800, 1)

    async def check_and_lock(self, key: str) -> bool:
        return await self.redis.set(key, 1, nx=True, ex=604800)

