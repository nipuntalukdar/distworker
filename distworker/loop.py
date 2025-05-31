import asyncio
import logging
import sys
import threading
import uuid
from asyncio import coroutines, ensure_future, futures
from concurrent.futures import Future
from time import sleep, time
from typing import Any, Dict, Union

from .dumpload import DumpLoad
from .redis_stream import RedisStream

LR = None
FAILED = False
logger = logging.getLogger("loop")
LOCK = threading.Lock()
EVENT = threading.Event()


class DistWorkerFuture(Future):
    def __init__(self):
        super().__init__()

    def result(self, timeout=None):
        return super().result(timeout)


"""
run_coroutine_threadsafe is copied from asyncio.run_coroutine_threadsafe
It is done as I need to return a customized future from the routine and
there is no way to extend that
"""


def run_coroutine_threadsafe(coro, loop):
    if not coroutines.iscoroutine(coro):
        raise TypeError("A coroutine object is required")
    future = DistWorkerFuture()

    def callback():
        try:
            futures._chain_future(ensure_future(coro, loop=loop), future)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            if future.set_running_or_notify_cancel():
                future.set_exception(exc)
            raise

    loop.call_soon_threadsafe(callback)
    return future


class StreamResponseHandler:
    def __init__(self):
        self._futures: Dict[str, asyncio.Future] = {}
        self._lock = threading.Lock()

    def add_future(self, local_id: str, fut: asyncio.Future):
        with self._lock:
            self._futures[local_id] = fut

    def remove_future(self, local_id: str):
        logger.debug("Removing future")
        with self._lock:
            self._futures.pop(local_id, None)

    def __call__(
        self, status: str, resp: Any, local_id: str, exception_str: Union[str, None]
    ):
        with self._lock:
            logger.debug(
                f"Got {resp} local_id={local_id}, status={status}, exception={not exception_str}"
            )
            fut = self._futures.pop(local_id, None)
            if fut:
                if exception_str:
                    exception = DumpLoad.load(exception_str)
                    fut.set_exception(exception)
                else:
                    fut.set_result((status, resp))


class LRegistry:
    def __init__(self, loop):
        self._loop = loop

    def response_handler(self, respone_handler):
        self._response_handler = respone_handler
        return self

    @property
    def loop(self):
        return self._loop

    def redis_stream(self, rs: RedisStream):
        self._rs = rs
        return self

    def reply_stream(self, reply_stream: str):
        self._reply_stream = reply_stream

    @property
    def rep_stream(self):
        return self._reply_stream

    async def submit_function(self, work: dict, retry=4) -> (str, Any):
        local_id = work["local_id"]
        fut = asyncio.Future()
        self._response_handler.add_future(local_id, fut)
        for i in range(retry):
            if await self._rs.enqueue_work(work):
                logger.debug("Enqueued task")
                result = await fut
                return result
            else:
                if i < retry - 1:
                    await asyncio.sleep(2)
                    continue
                logger.error("Removing future as task enqueue FAILED")
                self._response_handler.remove_future(local_id)
                return "EnqueueError", None


async def update_expire(stream: str, ttl: int, rs: RedisStream):
    while True:
        await asyncio.sleep(ttl)


async def main(configs: Dict[str, Any], loop):
    global LR, FAILED
    redis_host = configs.get("redis_host", "127.0.0.1")
    redis_port = configs.get("redis_port", 6379)
    taskstream = configs.get("taskstream", "tasks")
    taskgroup = configs.get("taskgroup", "taskgroup")
    reply_stream = configs.get("reply_stream", "replystream")
    reply_consumer = configs.get("reply_consumer", "replyconsumer")
    reply_consumer_group = configs.get("reply_consumer_group", "reply_consumer_group")
    reply_stream_alive_time = configs.get("reply_stream_alive_time", 3600)
    last_error_time = time()

    response_handler = StreamResponseHandler()
    redis_url = f"redis://{redis_host}:{redis_port}"

    rs = await RedisStream.create(
        redis_url=redis_url,
        task_stream=taskstream,
        task_group=taskgroup,
        respone_handler=response_handler,
    )
    if not rs:
        FAILED = True
        EVENT.set()
        return

    await rs.create_stream(reply_stream, reply_consumer_group)
    asyncio.create_task(update_expire(reply_stream, reply_stream_alive_time, rs))

    LR = LRegistry(loop)
    LR.response_handler(response_handler)
    LR.redis_stream(rs)
    LR.reply_stream(reply_stream)
    EVENT.set()

    while True:
        logger.debug("Dequeing responses")
        try:
            await rs.dequeue_response(
                reply_stream, reply_consumer, reply_consumer_group
            )
        except Exception:
            if time() - last_error_time > 4:
                last_error_time = time()
                logger.exception("Dequeue response error")
            await asyncio.sleep(1)


def start_event_loop(loop, configs: Dict[str, Any]):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main(configs, loop))


def distwork(func):
    def wrapper(*args, **kwargs):
        if not LR:
            raise "Loop is not initialized"
        task = DumpLoad.dumpfn(func, *args, **kwargs)
        work = {
            "work": task,
            "replystream": LR.rep_stream,
            "local_id": uuid.uuid4().hex,
        }
        fut = run_coroutine_threadsafe(LR.submit_function(work), LR.loop)
        return fut

    return wrapper


def start_loop(configs: Dict[str, Any]):
    with LOCK:
        global LR
        if LR:
            logger.info("Loop started already")
            return
        loop = asyncio.new_event_loop()
        thread = threading.Thread(
            target=start_event_loop, args=(loop, configs), daemon=True
        )
        thread.start()
        EVENT.wait()
        if FAILED:
            loop.close()
            sleep(2)
            logger.error("Startup failed")
            sys.exit(1)
