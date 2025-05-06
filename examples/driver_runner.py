import asyncio
import logging
import re
import signal
import sys
import uuid

from distworker.dumpload import DumpLoad
from distworker.redis_stream import RedisStream

logger = logging.getLogger("driver")
keep_running = True


def my_fun(x: int, y: int) -> int:
    return x + y


def my_fun2(num):
    return "even" if num % 2 == 0 else "odd"


def my_response_hanlder(resp):
    logger.info(f"Got the response {resp}")


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
            logger.info("Enter 'my_fun num1 num2' or 'my_fun2 num' or 'stop' ")
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
                work = DumpLoad.dumpfn(my_fun, *(num1, num2), **{})
                await rs.enqueue_work(
                    {
                        "work": work,
                        "replystream": astream,
                        "local_id": uuid.uuid4().hex,
                    }
                )
            elif elems[0] == "my_fun2":
                if len(elems) != 2:
                    logger.error("my_fun2 expectcs one number as input")
                    continue
                num = int(elems[1])
                work = DumpLoad.dumpfn(my_fun2, *(num,), **{})
                await rs.enqueue_work(
                    {
                        "work": work,
                        "replystream": astream,
                        "local_id": uuid.uuid4().hex,
                    }
                )
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
        rs = await RedisStream.create()
        await rs.create_stream(reply_stream, consumer_grp)
        response_task = asyncio.create_task(
            process_response(rs, reply_stream, "c1", consumer_grp)
        )
        my_queue = asyncio.Queue(100)
        inpu_task = asyncio.create_task(get_input(my_queue, rs, reply_stream))
        await response_task
        await inpu_task
    except KeyboardInterrupt:
        handle_error_interrupt()
    except Exception:
        handle_error_interrupt()
    finally:
        logger.info("Exiting ...")


if __name__ == "__main__":
    consumer_group = "agroup"
    reply_stream = "astream"
    asyncio.run(main(consumer_group, reply_stream))
