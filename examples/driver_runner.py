import asyncio
import csv
import logging
import os
import random
import re
import signal
import sys
import uuid
from asyncio import Future
from time import sleep
from typing import Any, Dict, Union

import pandas as pd

from distworker.dumpload import DumpLoad
from distworker.redis_stream import RedisStream

logger = logging.getLogger("driver")
keep_running = True
responses_dict: Dict[str, Future] = {}
TASK_STREAM = "tasks2"


def create_tmp_csv(rowc: int, outputfile: str):
    headers = ["name", "age", "income"]
    try:
        with open(outputfile, "wt") as fp:
            csvwriter = csv.writer(fp)
            csvwriter.writerow(headers)
            for i in range(rowc):
                csvwriter.writerow(
                    [
                        f"User {i + 1}",
                        random.randint(25, 102),
                        2000 + random.randint(1, 10000),
                    ]
                )
        return True
    except Exception as e:
        print(e)
        return False


def max_age_income(user_count: int) -> Union[Dict[str, Dict[str, int]], None]:
    tempfile = f"/tmp/test_{random.randint(999999, 999999999)}.csv"
    ret = create_tmp_csv(user_count, tempfile)
    if not ret:
        return None
    df = pd.read_csv(tempfile)
    max_income_row = df["income"].idxmax()
    max_income = df.loc[max_income_row, "income"]
    max_income_user = df.loc[max_income_row, "name"]
    max_age_row = df["age"].idxmax()
    max_age = df.loc[max_age_row, "age"]
    max_age_user = df.loc[max_age_row, "name"]
    os.remove(tempfile)
    return {
        "maxage": {"user": max_age_user, "age": int(max_age)},
        "maxincome": {"user": max_income_user, "income": int(max_income)},
    }


def my_fun(x: int, y: int) -> int:
    return x + y


def my_fun2(num):
    # simulate a long running task
    sleep(60)
    return "even" if num % 2 == 0 else "odd"


def my_response_hanlder(
    status: str, resp: Any, local_id: str, exception_str: str = None
):
    logger.debug(f"Got {status} {resp} {local_id}")
    fut = responses_dict.pop(local_id, None)
    if fut:
        if exception_str:
            exception = DumpLoad.load(exception_str)
            fut.set_exception(exception)
        else:
            fut.set_result((status, resp, exception_str))


async def process_response(
    rs: RedisStream, astream: str, consumer_id: str, consumer_grp: str
):
    global keep_running
    logger.info("Processing response")
    while keep_running:
        try:
            await rs.dequeue_response(
                astream, consumer_id, consumer_grp, my_response_hanlder
            )
        except asyncio.CancelledError:
            pass


async def get_input(input_queue: asyncio.Queue, rs: RedisStream, astream: str):
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    logger.info("Starting to get input")
    global keep_running
    while keep_running:
        try:
            logger.info(
                "Enter 'my_fun num1 num2' or 'my_fun2 num' or 'max_age_income num' or 'stop' "
            )
            line = await reader.readline()
            line = line.decode("utf-8").strip()
            if not line:
                continue
            elems = re.split(r"\s+", line)
            logger.info(elems)
            if elems[0] == "my_fun":
                if len(elems) != 3:
                    logger.error("my_fun expects two numbers as parameters")
                    continue
                num1 = float(elems[1])
                num2 = float(elems[2])
                args = DumpLoad.dumpargs(*(num1, num2), **{})
                func = DumpLoad.dumpfn(my_fun)

                local_id = uuid.uuid4().hex
                fut = Future()
                responses_dict[local_id] = fut
                if await rs.enqueue_work(
                    {
                        "args": args,
                        "func": func,
                        "replystream": astream,
                        "local_id": local_id,
                    },
                    TASK_STREAM,
                ):
                    logger.info("Awaiting result")
                    result = await fut
                    logger.info(f"The result {result}")
            elif elems[0] == "my_fun2":
                if len(elems) != 2:
                    logger.error("my_fun2 expectcs one number as input")
                    continue
                num = int(elems[1])
                func = DumpLoad.dumpfn(my_fun2)
                args = DumpLoad.dumpargs(*(num,), **{})

                local_id = uuid.uuid4().hex
                fut = Future()
                responses_dict[local_id] = fut
                if await rs.enqueue_work(
                    {
                        "func": func,
                        "args": args,
                        "replystream": astream,
                        "local_id": local_id,
                    },
                    TASK_STREAM,
                ):
                    result = await fut
                    logger.info(f"The result {result}")

            elif elems[0] == "max_age_income":
                if len(elems) != 2:
                    logger.error("max_age_income expectcs one number as input")
                    continue
                num = int(elems[1])
                func = DumpLoad.dumpfn(max_age_income)
                args = DumpLoad.dumpargs(*(num,), **{})

                local_id = uuid.uuid4().hex
                fut = Future()
                responses_dict[local_id] = fut
                if await rs.enqueue_work(
                    {
                        "func": func,
                        "args": args,
                        "replystream": astream,
                        "local_id": local_id,
                    },
                    TASK_STREAM,
                ):
                    result = await fut
                    logger.info(f"The result {result}")

            elif elems[0] == "stop":
                keep_running = False
            else:
                logger.info("Invalid input")

        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("error")


def handle_error_interrupt():
    logger.info("Shutting down")
    global keep_running
    keep_running = False
    for task in asyncio.all_tasks():
        if task is not asyncio.current_task():
            task.cancel()
    asyncio.gather(*asyncio.all_tasks())


async def main(consumer_grp: str, reply_stream: str):
    logger.info("Starting main")
    for sig in signal.SIGTERM, signal.SIGINT:
        asyncio.get_event_loop().add_signal_handler(sig, handle_error_interrupt)
    try:
        rs = await RedisStream.create(
            task_streams={TASK_STREAM: {"maxlen": 100}},
            respone_handler=my_response_hanlder,
        )
        await rs.create_stream(reply_stream, consumer_grp)
        response_task = asyncio.create_task(
            process_response(rs, reply_stream, "c1", consumer_grp)
        )
        my_queue = asyncio.Queue(100)
        inpu_task = asyncio.create_task(get_input(my_queue, rs, reply_stream))
        await response_task
        await inpu_task
    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt")
        handle_error_interrupt()
    except Exception:
        logger.exception("error")
        handle_error_interrupt()
    finally:
        logger.info("Exiting ...")


if __name__ == "__main__":
    consumer_group = "agroup"
    reply_stream = "astream"
    asyncio.run(main(consumer_group, reply_stream))
