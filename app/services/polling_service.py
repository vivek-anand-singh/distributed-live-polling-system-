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
    tt_seconds = 5
    _flush_started = False

    def __init__(self):
        self.redis_manager = RedisManager()

    async def vote(self, poll_id: str, option_id: str) -> None:
        """
        Registers a vote.
        Task 1: Store in memory.
        Task 2: Write to Redis immediately.
        Task 4: Buffer in memory (Batching).
        """
        self._memory_storage[poll_id][option_id] += 1

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
        return await self.get_complete_poll_data(poll_id)
        # raise NotImplemented

    async def flush_batch(self):
        """
        Task 4: Background process to flush memory buffer to Redis.
        """
        # TODO: Implement the batch flushing loop
        # 1. Loop forever (while True)
        # 2. Wait for BATCH_INTERVAL_SECONDS
        # 3. Flush _memory_storage to Redis
                # Prevent multiple flush loops
        if self._flush_started:
            return

        self._flush_started = True

        while True:
            await asyncio.sleep(settings.BATCH_INTERVAL_SECONDS)
            for poll_id, options in self._memory_storage.items():
                client = await self.redis_manager.get_client(poll_id)
                for option_id, count in options.items():
                    await client.hincrby(f"poll:{poll_id}", option_id, count)
            self._memory_storage = defaultdict(lambda:defaultdict(int))
        #raise NotImplemented
    
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

    async def get_complete_poll_data(self, poll_id: str) -> Dict[str, Any]:
        memory_storage_poll = self._memory_storage.get(poll_id, {})
        served_via = "app_cache"
        cached_results = self.get_cache(poll_id)
        if cached_results is not None:
            results = cached_results
        else:
            client, served_via = await self.redis_manager.get_client(poll_id)
            results = await client.hgetall(f"poll:{poll_id}")
            self.set_cache(poll_id, results)

        complete_results = {}

        for key, value in results.items():
            count = int(value)
            complete_results[key] = count

        for option_id, count in memory_storage_poll.items():
            complete_results[option_id] = complete_results.get(option_id, 0) + count

        return complete_results, served_via
