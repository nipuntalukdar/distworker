import asyncio
import logging
import threading
import time
import uuid
from concurrent.futures import Future

from distworker.dumpload import DumpLoad
from distworker.redis_stream import RedisStream

logger = logging.getLogger("driver")
rs = False


class MyResponseHandler:
    def __init__(self):
        self._future = Future()

    def __call__(self, resp):
        logger.debug(f"Setting response {resp}")
        self._future.set_result(resp)

    def get_result(self, timeout=None):
        return self._future.result(timeout=timeout)


def my_fun(x: int, y: int) -> int:
    return x + y


async def main():
    global rs
    rs = await RedisStream.create()
    await rs.create_stream("astream", "agroup")
    while True:
        await rs.dequeue_response("astream", "c1", "agroup", my_response_hanlder)


async def submit_function(work: dict, response_handler=None):
    await rs.enqueue_work(work, None, response_handler)


def my_response_hanlder(resp):
    logger.info(f"Got the response {resp}")


def start_event_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())


def get_input(loop):
    while True:
        logger.info("Reading input")
        x = [int(a) for a in input().split()]
        if len(x) < 2:
            logger.error("Give two numbers")
            continue
        task = DumpLoad.dumpfn(my_fun, *(x[0], x[1]), **{})
        resp_h = MyResponseHandler()
        work = {
            "work": task,
            "replystream": "astream",
            "local_id": uuid.uuid4().hex,
        }
        asyncio.run_coroutine_threadsafe(submit_function(work, resp_h), loop)
        resp = resp_h.get_result()
        logger.info(f"Result is: {resp}")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=start_event_loop, args=(loop,), daemon=True)
    thread.start()
    while not rs:
        time.sleep(0.2)
    get_input(loop)
    thread.join()
