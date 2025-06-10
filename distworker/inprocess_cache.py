import asyncio
import logging

from cachetools import TLRUCache

import distworker.configs.loggingconf as logconf

from .redis_stream import RedisStream

logger = logging.getLogger("InProcesCache")
logger.debug(f"LOG config {logconf.logg_conf_file()}")


class InProcessMessages:
    def __init__(
        self,
        rs: RedisStream,
        cache_name="message_id_cache",
        cache_max_size=1000000,
        ttl=86400,
    ):
        self._rs = rs
        self._max_cache_size = cache_max_size
        self._local_cache = TLRUCache(cache_max_size, self.cache_ttu)
        self._cache_ttl = ttl
        self._cache_name = cache_name

    async def is_in_cache(self, key) -> int:
        if key in self._local_cache:
            return True
        try:
            ret = await self._rs.hkey_expiretime(self._cache_name, key)
            if ret > 0:
                self._local_cache[key] = ret
            return ret
        except Exception:
            logger.exception("Error getting expire time")
            return -2

    def cache_ttu(self, key, value, time_value):
        logger.debug(f"Called ttu for {key}, {time_value} {self._cache_ttl}")
        if value == -1:
            return time_value + self._cache_ttl
        else:
            return time_value + value

    async def delete_from_cache(self, key):
        self._local_cache.pop(key)
        await self._rs.del_hkey(self._cache_name, key)

    async def add_to_cache(self, message_id: str, retry: int = 3) -> bool:
        result = False
        if retry < -1:
            retry = 1

        while retry > 0:
            result = await self._rs.add_to_hset(
                self._cache_name, message_id, str(1), self._cache_ttl
            )
            if result:
                self._local_cache[message_id] = -1
                return True
            await asyncio.sleep(10)
            retry -= 1

        return False
