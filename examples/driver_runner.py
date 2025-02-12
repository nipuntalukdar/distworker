import asyncio
import uuid
import pandas as pd
from random import randint
import logging

import distworker.configs.loggingconf
from distworker.redis_stream import RedisStream
from distworker.dumpload import DumpLoad

logger = logging.getLogger("driver")


def my_fun(x: int, y: int) -> int:
    return x + y


def my_response_hanlder(resp):
    logger.info(f"Got the response {resp}")


async def main():
    rs = await RedisStream.create()
    await rs.create_stream("astream", "agroup")

    for i in range(100):
        work = DumpLoad.dumpfn(my_fun, *(randint(1, 100), randint(2, 300)), **{})
        await rs.enqueue_work(
            {"work": work, "replystream": "astream", "local_id": uuid.uuid4().hex}
        )
    logging.info("Getting response")
    while True:
        await rs.dequeue_response("astream", "c1", "agroup", my_response_hanlder)


if __name__ == "__main__":
    work = DumpLoad.dumpfn(my_fun, *(randint(1, 100), randint(2, 300)), **{})
    asyncio.run(main())
