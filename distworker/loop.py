import asyncio
import logging
import threading
import uuid
from typing import Any, Dict

from .dumpload import DumpLoad
from .redis_stream import RedisStream

lr = None

logger = logging.getLogger("loop")
lock = threading.Lock()
event = threading.Event()


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

    def __call__(self, status: str, resp: Any, local_id: str):
        with self._lock:
            logger.debug(f"Got {resp} for local_id={local_id}, status={status}")
            fut = self._futures.pop(local_id, None)
            if fut:
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

    async def submit_function(self, work: dict) -> (str, Any):
        local_id = work["local_id"]
        fut = asyncio.Future()
        self._response_handler.add_future(local_id, fut)
        if await self._rs.enqueue_work(work):
            logger.debug("Enqueued task")
            result = await fut
            return result
        else:
            logger.error("Removing future as task enqueue failed")
            self._response_hanlder.remove_future(local_id)
            return "EnqueueError", None


async def update_expire(stream: str, ttl: int, rs: RedisStream):
    while True:
        await asyncio.sleep(ttl)


async def main(configs: Dict[str, Any], loop):
    global lr
    redis_host = configs.get("redis_host", "127.0.0.1")
    redis_port = configs.get("redis_port", 6379)
    taskstream = configs.get("taskstream", "tasks")
    taskgroup = configs.get("taskgroup", "taskgroup")
    reply_stream = configs.get("reply_stream", "replystream")
    reply_consumer = configs.get("reply_consumer", "replyconsumer")
    reply_consumer_group = configs.get("reply_consumer_group", "reply_consumer_group")
    reply_stream_alive_time = configs.get("reply_stream_alive_time", 3600)

    response_handler = StreamResponseHandler()
    redis_url = f"redis://{redis_host}:{redis_port}"

    rs = await RedisStream.create(
        redis_url=redis_url,
        task_stream=taskstream,
        task_group=taskgroup,
        respone_handler=response_handler,
    )
    await rs.create_stream(reply_stream, reply_consumer_group)
    asyncio.create_task(update_expire(reply_stream, reply_stream_alive_time, rs))

    lr = LRegistry(loop)
    lr.response_handler(response_handler)
    lr.redis_stream(rs)
    lr.reply_stream(reply_stream)
    event.set()

    while True:
        logger.debug("Dequeing responses")
        await rs.dequeue_response(reply_stream, reply_consumer, reply_consumer_group)


def start_event_loop(loop, configs: Dict[str, Any]):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main(configs, loop))


def distwork(func):
    def wrapper(*args, **kwargs):
        if not lr:
            raise "Loop is not initialized"
        task = DumpLoad.dumpfn(func, *args, **kwargs)
        work = {
            "work": task,
            "replystream": lr.rep_stream,
            "local_id": uuid.uuid4().hex,
        }
        fut = asyncio.run_coroutine_threadsafe(lr.submit_function(work), lr.loop)
        return fut

    return wrapper


def start_loop(configs: Dict[str, Any]):
    with lock:
        global lr
        if lr:
            logger.info("Loop started already")
            return
        loop = asyncio.new_event_loop()
        thread = threading.Thread(
            target=start_event_loop, args=(loop, configs), daemon=True
        )
        thread.start()
        event.wait()
