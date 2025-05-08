import asyncio
import logging
from typing import Callable

import redis.asyncio as redis
from redis.exceptions import ResponseError

from .dumpload import DumpLoad

logger = logging.getLogger("stream")


class RedisStream:
    def __init__(self, redis_client, redis_url, task_stream, task_group, maxlen):
        self._redis_url = redis_url
        self._task_stream = task_stream
        self._task_group = task_group
        self._maxlen = maxlen
        self._redis: redis.StrictRedis = redis_client
        self._response_handlers = {}

    @classmethod
    async def create(
        cls,
        redis_url="redis://127.0.0.1:6379",
        task_stream="tasks",
        task_group="taskgroup",
        maxlen=1000000,
    ):
        redis_client = await redis.StrictRedis.from_url(redis_url)
        try:
            await redis_client.xgroup_create(task_stream, task_group, mkstream=True)
        except ResponseError as e:
            if str(e) != "BUSYGROUP Consumer Group name already exists":
                logger.exception("error")
                exit(1)
        except Exception:
            logger.exception("error")
            exit(1)
        return cls(redis_client, redis_url, task_stream, task_group, maxlen)

    async def set(self, key: str, value: str):
        return await self._redis.set(key, value)

    async def create_stream(self, key: str, group: str, expiry: int = 86400) -> bool:
        try:
            async with self._redis.pipeline() as pipe:
                pipe.xgroup_create(key, group, mkstream=True)
                pipe.expire(key, expiry)
                results = await pipe.execute(raise_on_error=False)
                for result in results:
                    if isinstance(result, bool) and not result:
                        logger.error("Pipeline execute")
                        return False
                    elif isinstance(result, ResponseError):
                        if (
                            str(result)
                            != "BUSYGROUP Consumer Group name already exists"
                        ):
                            return False
                        return False
                return True
        except Exception:
            logger.exception("error")
            return False

    async def add_to_hset(self, hkey, key, val):
        try:
            await self._redis.hset(hkey, key, val)
        except Exception:
            logger.exception("error")
            return False
        return True

    async def del_stream_group(self, key: str, group: str) -> bool:
        try:
            with self._redis.pipeline() as pipe:
                pipe.xgroup_destroy(key, group)
                pipe.delete(key)
                pipe.execute()
            return True
        except Exception:
            logger.exception("error")
            return False

    async def enqueue_work(
        self, work: dict, stream: str = None, resp_handler=None
    ) -> bool:
        the_stream = stream
        if not the_stream:
            the_stream = self._task_stream
        try:
            await self._redis.xadd(the_stream, work)
            if resp_handler:
                self._response_handlers[work["local_id"]] = resp_handler
            return True
        except Exception:
            logger.exception("error")
            return False

    async def dequeue_work(
        self, worker_func: Callable, consumer_id: str, max_work: int = 5
    ):
        works = await self._redis.xreadgroup(
            self._task_group, consumer_id, {self._task_stream: ">"}, max_work, 10000
        )
        if not works:
            return True
        for stream, messages in works:
            for message in messages:
                message_id, message_data = message
                if not message_data or b"work" not in message_data:
                    self._redis.xack(self._task_stream, self._task_group, message_id)
                    continue
                ret, ok = await worker_func(message_data[b"work"])
                if (
                    b"replystream" in message_data
                    and await self.enqueue_work(
                        {
                            "response": DumpLoad.dump(ret),
                            "status": DumpLoad.dump(ok),
                            "local_id": message_data[b"local_id"],
                        },
                        message_data[b"replystream"],
                    )
                    or b"replystream" not in message_data
                ):
                    await self._redis.xack(
                        self._task_stream, self._task_group, message_id
                    )
        return True

    async def dequeue_response(
        self,
        stream: str,
        consumer_id: str,
        consumer_group: str,
        callback: Callable = None,
        max_response: int = 100,
        max_wait=1000,
    ):
        responses = await self._redis.xreadgroup(
            consumer_group, consumer_id, {stream: ">"}, max_wait, max_response, True
        )
        if not responses:
            return True
        for stream, messages in responses:
            for message in messages:
                message_id, message_data = message
                if (
                    not message_data
                    or b"response" not in message_data
                    and b"status" not in message_data
                    and b"local_id" not in message_data
                ):
                    continue
                response = DumpLoad.load(message_data[b"response"])
                status = DumpLoad.load(message_data[b"status"])
                local_id = message_data[b"local_id"].decode()
                if status == "OK":
                    if local_id in self._response_handlers:
                        try:
                            self._response_handlers[local_id](response)
                            del self._response_handlers[local_id]
                        except:
                            pass
                    elif callback:
                        try:
                            callback(response)
                        except:
                            pass
                else:
                    logger.error("Execution failed")
                await self._redis.xack(stream, consumer_group, message_id)
                logger.debug(f"Response {response}")

    async def trim(self):
        try:
            await self._redis.xtrim(self._task_stream, self._maxlen)
        except Exception:
            logger.exception("error")

    async def get(self, key) -> str:
        return self._redis.get(key)

    async def del_hkey(self, hkey, subkey):
        try:
            await self._redis.hdel(hkey, subkey)
            return True
        except Exception:
            logger.exception(f"Key deleting issue: {hkey} {subkey}")
            return False


async def main():
    a = RedisStream(maxlen=200000)
    for i in range(10):
        a.enqueue_work({"a": i, "b": i * 2})
    a.dequeue_work()
    a.trim()


if __name__ == "__main__":
    asyncio.run(main())
