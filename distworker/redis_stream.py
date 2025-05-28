import asyncio
import logging
from typing import Callable, Self

import redis.asyncio as redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import (
    BusyLoadingError,
    ConnectionError,
    ResponseError,
    TimeoutError,
)
from redis.retry import Retry

from .dumpload import DumpLoad

logger = logging.getLogger("stream")


class RedisStream:
    def __init__(
        self,
        redis_client: redis.Redis,
        redis_url: str,
        task_stream: str,
        task_group: str,
        maxlen: int,
        respone_handler: Callable = None,
        worker_response_queue: asyncio.Queue = None,
    ):
        self._redis_url = redis_url
        self._task_stream = task_stream
        self._task_group = task_group
        self._maxlen = maxlen
        self._redis: redis.StrictRedis = redis_client
        self._response_handler: Callable = respone_handler
        self._worker_response_queue = worker_response_queue

    @classmethod
    async def create(
        cls,
        redis_url: str = "redis://127.0.0.1:6379",
        task_stream: str = "tasks",
        task_group: str = "taskgroup",
        maxlen: int = 1000000,
        worker_response_queue: asyncio.Queue = None,
        respone_handler: Callable = None,
    ) -> Self:
        retry_strategy = Retry(
            ExponentialBackoff(
                cap=5, base=0.1
            ),  # Max 5 seconds delay, starts with 0.1s
            retries=20,  # Try up to 20 times
            supported_errors=(ConnectionError, TimeoutError, BusyLoadingError),
        )
        retry_on_errors = [ConnectionError, TimeoutError, BusyLoadingError]
        redis_client = await redis.StrictRedis.from_url(
            redis_url,
            retry=retry_strategy,
            retry_on_error=retry_on_errors,
            socket_connect_timeout=20,
            socket_timeout=20,
            health_check_interval=40,
        )
        try:
            await redis_client.xgroup_create(task_stream, task_group, mkstream=True)
        except ResponseError as e:
            if str(e) != "BUSYGROUP Consumer Group name already exists":
                logger.exception("error")
                exit(1)
        except Exception:
            logger.exception("error")
            exit(1)
        return cls(
            redis_client, redis_url, task_stream, task_group, maxlen, respone_handler
        )

    async def set(self, key: str, value: str):
        return await self._redis.set(key, value)

    async def expire(self, key: str, ttl: int):
        try:
            await self._redis.expire(key, ttl)
        except Exception:
            logger.exception("Error setting expiry")

    def set_response_queue(self, queue: asyncio.Queue):
        self._worker_response_queue = queue

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

    async def response_processor(self):
        while True:
            (
                ret,
                ok,
                replystream,
                local_id,
                message_id,
            ) = await self._worker_response_queue.get()

            if (
                replystream
                and await self.enqueue_work(
                    {
                        "response": DumpLoad.dump(ret),
                        "status": DumpLoad.dump(ok),
                        "local_id": local_id,
                    },
                    replystream,
                )
            ) or not replystream:
                await self._redis.xack(self._task_stream, self._task_group, message_id)
            self._worker_response_queue.task_done()

    async def add_to_hset(self, hkey, key, val, expiretime=0):
        try:
            if expiretime > 0:
                async with self._redis.pipeline() as pipe:
                    pipe.hset(hkey, key, val)
                    pipe.hexpire(hkey, expiretime, key)
                    await pipe.execute()
            else:
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

    async def enqueue_work(self, work: dict, stream: str = None) -> bool:
        the_stream = stream
        if not the_stream:
            the_stream = self._task_stream
        try:
            await self._redis.xadd(the_stream, work)
            return True
        except Exception:
            logger.exception("error")
            return False

    async def process_work(self, messages, worker_func, pending=False):
        for message in messages:
            message_id, message_data = message
            logger.debug(
                f"Processing message, message id {message_id}, pending={pending}"
            )
            if not message_data or b"work" not in message_data:
                await self._redis.xack(self._task_stream, self._task_group, message_id)
                continue
            await worker_func(
                message_data[b"work"],
                message_data.get(b"replystream", None),
                message_data[b"local_id"],
                message_id,
            )

    async def get_pending_work(
        self,
        worker_func: Callable,
        consumer_id: str,
        max_work: int = 20,
        min_idle_time=7200000,
    ):
        logger.debug(
            f"Getting pending tasks with min_idle_time {min_idle_time} for consumer_id {consumer_id}"
        )
        try:
            pending_messages = await self._redis.xpending_range(
                self._task_stream,
                self._task_group,
                "-",
                "+",
                max_work,
                consumer_id,
                min_idle_time,
            )
            if pending_messages:
                message_ids = [
                    msg["message_id"]
                    for msg in pending_messages
                    if not worker_func.is_in_process(msg["message_id"])
                ]
                if message_ids:
                    messages = await self._redis.xclaim(
                        self._task_stream,
                        self._task_group,
                        consumer_id,
                        min_idle_time=min_idle_time,
                        message_ids=message_ids,
                    )
                    await self.process_work(messages, worker_func, pending=True)
            else:
                logger.debug(
                    f"No pending tasks with min_idle_time {min_idle_time} for consumer_id {consumer_id}"
                )

        except Exception:
            logger.exception("Getting pending work")

    async def dequeue_work(
        self,
        worker_func: Callable,
        consumer_id: str,
        max_work: int = 20,
        max_wait_milli_second=10000,
    ):
        works = None
        try:
            works = await self._redis.xreadgroup(
                self._task_group,
                consumer_id,
                {self._task_stream: ">"},
                max_work,
                max_wait_milli_second,
            )
        except TimeoutError:
            logger.debug("Read timeout")
        if not works:
            logger.debug("Did not receive any tasks")
            return True
        for stream, messages in works:
            logger.debug(f"Num tasks fetched {len(messages)} in stream {stream}")
            await self.process_work(messages, worker_func)
        return True

    async def dequeue_response(
        self,
        stream: str,
        consumer_id: str,
        consumer_group: str,
        callback: Callable = None,
        max_response: int = 100,
        max_wait=10000,
    ):
        responses = None
        logger.debug(
            f"Dequing response stream={stream}, group={consumer_group}, consumer_id={consumer_id}"
        )
        try:
            responses = await self._redis.xreadgroup(
                consumer_group,
                consumer_id,
                {stream: ">"},
                max_response,
                max_wait,
                False,
            )
        except TimeoutError:
            await asyncio.sleep(0.01)
        if not responses:
            logger.debug("No responses received")
            return True
        logger.debug("responses received")
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
                    if local_id:
                        try:
                            logger.info("Calling response handler")
                            self._response_handler(status, response, local_id)
                        except Exception:
                            logger.exception("Response handling")
                    elif callback:
                        try:
                            logger.info("Callback for response")
                            callback(response)
                        except Exception:
                            logger.exception("Callback for response")
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
