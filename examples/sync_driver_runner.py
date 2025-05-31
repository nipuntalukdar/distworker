import asyncio
import logging
import threading
import time
import uuid
from typing import Any, Dict

from distworker.dumpload import DumpLoad
from distworker.redis_stream import RedisStream


class MyResponseHandler:
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
        self, status: str, resp: Any, local_id: str, exception_str: str = None
    ):
        with self._lock:
            logger.debug(f"Got {resp} for local_id={local_id}, status={status}")
            fut = self._futures.pop(local_id, None)
            if fut:
                if exception_str:
                    exception = DumpLoad.load(exception_str)
                    fut.set_exception(exception)
                else:
                    fut.set_result((status, resp))

    def get_result(self, timeout=None):
        return self._future.result(timeout=timeout)


logger = logging.getLogger("driver")
rs = False
my_response_hanlder = MyResponseHandler()


def my_divide(x: int, y: int) -> int:
    return x / y


async def main():
    global rs
    rs = await RedisStream.create(respone_handler=my_response_hanlder)
    await rs.create_stream("astream", "agroup")
    while True:
        logger.debug("Dequeing responses")
        await rs.dequeue_response("astream", "c1", "agroup")


async def submit_function(work: dict) -> (str, Any):
    local_id = work["local_id"]
    fut = asyncio.Future()
    my_response_hanlder.add_future(local_id, fut)
    if await rs.enqueue_work(work):
        logger.debug("Enqueued task")
        result = await fut
        return result
    else:
        logger.error("Removing future as task enqueue failed")
        my_response_hanlder.remove_future(local_id)
        return "EnqueueError", None


def my_response_processor(status: str, resp: Any, local_id: str):
    logger.info(f"Got response={resp} status={status} local_id={local_id}")


def start_event_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())


def get_input(loop):
    while True:
        logger.info("my_divide <num1> / <num2>, Give num1 and num2:  ")
        x = [int(a) for a in input().split()]
        if len(x) < 2:
            logger.error("Give two numbers")
            continue
        task = DumpLoad.dumpfn(my_divide, *(x[0], x[1]), **{})
        work = {
            "work": task,
            "replystream": "astream",
            "local_id": uuid.uuid4().hex,
        }
        fut = asyncio.run_coroutine_threadsafe(submit_function(work), loop)
        logger.info(f"Here is the response {fut.result()}")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=start_event_loop, args=(loop,), daemon=True)
    thread.start()
    while not rs:
        time.sleep(0.2)
    get_input(loop)
    thread.join()
