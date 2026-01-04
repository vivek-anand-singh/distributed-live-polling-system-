import asyncio
from collections import defaultdict
from typing import Any, Dict
from typing import Tuple
from app.core.config import settings
from app.core.redis_manager import RedisManager
import time


class PollingService:
    # In-memory storage for Task 1 & Task 4 (Batch buffer)
    _memory_storage = defaultdict(lambda: defaultdict(int))
    TTL_CACHE = {}  # Define TTL_cache as a class variable
    tt_seconds = 100
    client = None  # Define client as a class variable

    def __init__(self):
        self.redis_manager = RedisManager()

    async def get_client(self, poll_id: str):
        if self.client is None:
            self.client = await self.redis_manager.get_client(poll_id)
        return self.client

    async def vote(self, poll_id: str, option_id: str) -> None:
        """
        Registers a vote.
        Task 1: Store in memory.
        Task 2: Write to Redis immediately.
        Task 4: Buffer in memory (Batching).
        """
        client = await self.get_client(poll_id)
        await client.hincrby(f"poll:{poll_id}", option_id, 1)

    async def get_results(self, poll_id: str) -> tuple[Dict[str, int], str]:
        """
        Get results.
        Task 1: Read from memory.
        Task 2: Read from Redis.
        Task 3: Check App Cache -> Redis.
        Task 4: Redis + Memory Buffer.
        """
        # TODO: Implement result fetching logic
        # Should return a dictionary like {"OptionA": 5, "OptionB": 3}
        # return dict(self._memory_storage.get(poll_id, {}))
        served_via = "app_cache"
        cached_results = self.get_cache(poll_id)
        if cached_results is not None:
            return cached_results, served_via
        served_via = "redis"
        client = await self.get_client(poll_id)
        results = await client.hgetall(f"poll:{poll_id}")
        self.set_cache(poll_id, results)
        return results, served_via
        # raise NotImplemented

    async def flush_batch(self):
        """
        Task 4: Background process to flush memory buffer to Redis.
        """
        # TODO: Implement the batch flushing loop
        # 1. Loop forever (while True)
        # 2. Wait for BATCH_INTERVAL_SECONDS
        # 3. Flush _memory_storage to Redis
        raise NotImplemented
    
    def set_cache(self, key: str, value: Dict[str, int]) -> None:
        expires_at = time.time() + self.tt_seconds    
        self.TTL_CACHE[key] = (value, expires_at)

    def get_cache(self, key: str) -> Dict[str, int] | None:
        entry = self.TTL_CACHE.get(key)
        if not entry:
            return None
        value, expires_at = entry
        if time.time() > expires_at:
            del self.TTL_CACHE[key]  # cleanup
            return None
        return value
