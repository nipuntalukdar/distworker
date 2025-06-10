import asyncio
import logging
from typing import Any, Callable, Dict, List, Self, Tuple, Union

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
        redis_url: str = "redis://127.0.0.1:6379",
        task_streams: Dict[str, Any] = {"tasks": {"maxlen": 100}},
        task_group: str = "taskgroup",
        maxlen: int = 100,
        respone_handler: Callable = None,
        worker_response_queue: asyncio.Queue = None,
    ):
        self._redis_url = redis_url
        self._task_streams = task_streams
        self._task_group = task_group
        self._maxlen = maxlen
        self._redis: redis.StrictRedis = redis_client
        self._response_handler: Callable = respone_handler
        self._worker_response_queue = worker_response_queue

    @classmethod
    async def create(
        cls,
        redis_url: str = "redis://127.0.0.1:6379",
        task_streams: Dict[str, Any] = {"tasks": {"maxlen": 100}},
        task_group: str = "taskgroup",
        maxlen: int = 100,
        worker_response_queue: asyncio.Queue = None,
        respone_handler: Callable = None,
    ) -> Self:
        retry_strategy = Retry(
            ExponentialBackoff(
                cap=5, base=0.1
            ),  # Max 5 seconds delay, starts with 0.1s
            retries=20,  # Try up to 20 times
            supported_errors=(
                ConnectionError,
                TimeoutError,
                BusyLoadingError,
                ConnectionRefusedError,
            ),
        )
        retry_on_errors = [
            ConnectionError,
            TimeoutError,
            BusyLoadingError,
            ConnectionRefusedError,
        ]
        redis_client = await redis.StrictRedis.from_url(
            redis_url,
            retry=retry_strategy,
            retry_on_error=retry_on_errors,
            socket_connect_timeout=20,
            socket_timeout=20,
            health_check_interval=40,
        )
        connection_attempts = 10
        for i in range(connection_attempts):
            failed = False
            for task_stream in task_streams.keys():
                try:
                    await redis_client.xgroup_create(
                        task_stream, task_group, mkstream=True
                    )
                except (ConnectionRefusedError, ConnectionError):
                    logger.error("Unable to connect to Redis")
                    if i == connection_attempts - 1:
                        logger.error("Unable to connect to Redis, giving up ...")
                        return None
                    else:
                        await asyncio.sleep(3)
                        failed = True
                        break
                except ResponseError as e:
                    if str(e) != "BUSYGROUP Consumer Group name already exists":
                        logger.exception("error")
                except Exception:
                    logger.exception("error")
                    return None
                logger.info(f"Created {task_stream} and group {task_group}")
            if not failed:
                break
        return cls(
            redis_client, redis_url, task_streams, task_group, maxlen, respone_handler
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
                exception_str,
                replystream,
                local_id,
                message_id,
                stream,
            ) = await self._worker_response_queue.get()
            exception = True if exception_str else False
            logger.debug(
                f"Response from queue = {ret}, {ok}, {replystream}, {local_id}, {exception}"
            )

            if not replystream:
                logger.debug(
                    f"Not sending reply for local_id {local_id} as no replystream given"
                )

            if (
                replystream
                and await self.enqueue_work(
                    {
                        "response": DumpLoad.dump(ret),
                        "status": DumpLoad.dump(ok),
                        "local_id": local_id,
                        "exception_str": exception_str,
                    },
                    replystream,
                )
            ) or not replystream:
                await self._redis.xack(stream, self._task_group, message_id)
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
            async with self._redis.pipeline() as pipe:
                pipe.xgroup_destroy(key, group)
                pipe.delete(key)
                await pipe.execute()
            return True
        except Exception:
            logger.exception("error")
            return False

    async def enqueue_work(self, work: dict, the_stream: str) -> bool:
        try:
            if not work.get("exception_str", None):
                # Remove empty or None value as None will cause xadd to fail
                work.pop("exception_str", None)
            logger.debug("Enqueueing task ")
            val = await self._redis.xadd(the_stream, work)
            logger.debug(f"Task enqueued {val}")
            return True

        except Exception:
            logger.exception("error")
            return False

    async def process_work(
        self,
        stream: str,
        messages: List[Tuple[str, str]],
        worker_func: Callable,
        pending=False,
    ):
        for message in messages:
            message_id, message_data = message
            logger.debug(
                f"Processing message, message id {message_id}, pending={pending}"
            )
            if not message_data or (
                b"func" not in message_data and b"func_cache_id" not in message_data
            ):
                await self._redis.xack(self._task_streams, self._task_group, message_id)
                continue
            await worker_func(
                stream,
                message_data.get(b"func", None),
                message_data.get(b"args", None),
                message_data.get(b"replystream", None),
                message_data[b"local_id"],
                message_id,
                message_data.get(b"func_cache_id", None),
            )

    async def get_pending_work(
        self,
        worker_func: Callable,
        consumer_id: str,
        max_work: int = 20,
        min_idle_time=7200000,
        task_streams: Dict[str, Any] = {},
    ):
        for the_stream in task_streams.keys():
            await self.get_pending_work_stream(
                worker_func, the_stream, consumer_id, max_work, min_idle_time
            )

    async def get_pending_work_stream(
        self, worker_func, the_stream, consumer_id, max_work, min_idle_time
    ):
        logger.debug(
            f"Getting pending tasks with min_idle_time {min_idle_time} for consumer_id {consumer_id}"
        )
        try:
            pending_messages = await self._redis.xpending_range(
                the_stream,
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
                        the_stream,
                        self._task_group,
                        consumer_id,
                        min_idle_time=min_idle_time,
                        message_ids=message_ids,
                    )
                    await self.process_work(
                        the_stream, messages, worker_func, pending=True
                    )
            else:
                logger.debug(
                    f"No pending tasks with min_idle_time {min_idle_time} for consumer_id {consumer_id}"
                )

        except Exception:
            logger.exception("Getting pending work")

    async def delete_stream(self, stream):
        try:
            num_key_deleted = await self._redis.delete(stream)
            if num_key_deleted == 0:
                logger.debug(f"Perhaps stream: {stream} is already deleted")
            return True
        except Exception:
            logger.exception("deleting key")
            return False

    async def dequeue_work(
        self,
        worker_func: Callable,
        consumer_id: str,
        max_work: int = 20,
        max_wait_milli_second=10000,
        task_stream_details: Dict[str, str] = None,
    ):
        works = None
        if not task_stream_details:
            task_stream_details = {stream: ">" for stream in self._task_streams.keys()}
        logger.debug(f"The task details={task_stream_details}")
        try:
            works = await self._redis.xreadgroup(
                self._task_group,
                consumer_id,
                task_stream_details,
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
            await self.process_work(stream, messages, worker_func)
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
                exception_str = message_data.get(b"exception_str", None)
                if local_id:
                    try:
                        logger.info("Calling response handler")
                        self._response_handler(
                            status, response, local_id, exception_str
                        )
                    except Exception:
                        logger.exception("Response handling")
                elif callback:
                    try:
                        logger.info("Callback for response")
                        callback(response)
                    except Exception:
                        logger.exception("Callback for response")
                await self._redis.xack(stream, consumer_group, message_id)
                logger.debug(f"Response {response}")

    async def trim(self):
        try:
            await self._redis.xtrim(self._task_streams, self._maxlen)
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

    async def setval_expiry(self, key, value, expiry):
        try:
            return await self._redis.setex(key, expiry, value)
        except Exception:
            logger.exception("Problem in setting value and expiry")
            return False

    async def hkey_expiretime(self, hkey, key):
        _, expiretime = await self._redis.hexpiretime(hkey, key)
        return expiretime

    async def getval_expiry(self, key) -> Tuple[Union[str, None], Union[str, None]]:
        try:
            async with self._redis.pipeline() as pipe:
                val, expiry = await pipe.get(key).ttl(key).execute()
                logger.debug(f"{key} {expiry} {val}")
                return val, expiry
        except Exception:
            logger.exception("Problem in getting value and expiry")
            return None, None


async def main():
    a = RedisStream(maxlen=200000)
    for i in range(10):
        a.enqueue_work({"a": i, "b": i * 2})
    a.dequeue_work()
    a.trim()


if __name__ == "__main__":
    asyncio.run(main())
