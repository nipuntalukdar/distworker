import argparse
import asyncio
import logging
import threading
import time
import uuid
from concurrent.futures import as_completed
from random import randint
from typing import Any, Dict

from distworker.dumpload import DumpLoad
from distworker.redis_stream import RedisStream
from distworker.utils import get_configs_from_file, get_redis_url

GATHER_RESULT_ONE_SHOT = False
STOPPED = False
NO_RESPONSE_NEEDED = False


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
    task_streams = {"tasks1": {"maxlen": 100}}
    configs = get_configs_from_file("/usr/share/distworker/client_configs.json")
    redis_url = get_redis_url(configs)
    redis_ssl = configs.get("redis_ssl")
    if not redis_ssl:
        rs = await RedisStream.create(
            redis_url=redis_url,
            task_streams=task_streams,
            respone_handler=my_response_hanlder,
        )
    else:
        rs = await RedisStream.create(
            redis_url=redis_url,
            task_streams=task_streams,
            respone_handler=my_response_hanlder,
            ssl_ca_certs=configs.get("ssl_ca_certs"),
            ssl_certfile=configs.get("ssl_certfile"),
            ssl_keyfile=configs.get("ssl_keyfile"),
        )
    await rs.create_stream("astream", "agroup")
    while not STOPPED:
        logger.debug("Dequeing responses")
        await rs.dequeue_response("astream", "c1", "agroup")
    await rs.del_stream_group("astream", "agroup")


async def submit_function(work: dict) -> (str, Any):
    local_id = work["local_id"]
    response_needed = work.get("replystream", None) is not None
    fut = None
    if response_needed:
        fut = asyncio.Future()
        my_response_hanlder.add_future(local_id, fut)
    if await rs.enqueue_work(work, "tasks1"):
        logger.debug("Enqueued task")
        if response_needed:
            result = await fut
            return result
        else:
            logger.debug("Response from queue is not needed")
            return "Enqueued", None
    else:
        if response_needed:
            logger.error("Removing future as task enqueue failed")
            my_response_hanlder.remove_future(local_id)
        return "EnqueueError", None


def my_response_processor(status: str, resp: Any, local_id: str):
    logger.info(f"Got response={resp} status={status} local_id={local_id}")


def start_event_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())


def get_input(loop):
    futures = []
    count = 100
    func = DumpLoad.dumpfn(my_divide)
    while not GATHER_RESULT_ONE_SHOT or count > 0:
        if not GATHER_RESULT_ONE_SHOT:
            logger.info("my_divide <num1> / <num2>, Give num1 and num2:  ")
            x = [int(a) for a in input().split()]
            if len(x) < 2:
                logger.error("Give two numbers")
                continue
            args = DumpLoad.dumpargs(*(x[0], x[1]), **{})
        else:
            count -= 1
            args = DumpLoad.dumpargs(*(randint(2, 199999), randint(2, 99999)), **{})
        if NO_RESPONSE_NEEDED:
            work = {
                "func": func,
                "args": args,
                "local_id": uuid.uuid4().hex,
            }
        else:
            work = {
                "func": func,
                "args": args,
                "replystream": "astream",
                "local_id": uuid.uuid4().hex,
            }
        fut = asyncio.run_coroutine_threadsafe(submit_function(work), loop)
        if not GATHER_RESULT_ONE_SHOT:
            logger.info(f"Here is the response {fut.result()}")
        else:
            futures.append(fut)
    i = 1
    if futures:
        for fut in as_completed(futures):
            print(f"Result {i}: {fut.result()}")
            i += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="sync_driver")
    parser.add_argument(
        "--gather", action="store_true", help="Collect all results in one shot"
    )
    parser.add_argument(
        "--response_not_needed",
        action="store_true",
        help="No response needed from the work queue executor",
    )
    args = parser.parse_args()
    GATHER_RESULT_ONE_SHOT = args.gather
    NO_RESPONSE_NEEDED = args.response_not_needed

    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=start_event_loop, args=(loop,), daemon=True)
    thread.start()
    while not rs:
        time.sleep(0.2)
    get_input(loop)
    if GATHER_RESULT_ONE_SHOT:
        STOPPED = True
    thread.join()
