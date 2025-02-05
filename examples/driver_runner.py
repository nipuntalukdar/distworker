import asyncio
from random import randint
import logging

import distworker.configs.loggingconf
from distworker.redis_stream import RedisStream
from distworker.dumpload import DumpLoad

logger = logging.getLogger("driver")

def my_fun(x: int = 1, y: int = 2):
    logging.info(f"Hello {x} {y}")
    return  x + y


async def main():
    rs = await RedisStream.create()
    await rs.create_stream("astream", "agroup")

    for i in range(100):
        work = DumpLoad.dumpfn(my_fun, *(randint(1, 100), randint(2, 300)), **{})
        await rs.enqueue_work({"work": work, "replystream": "astream"})
    logging.info("Getting response")
    while True:
        await rs.dequeue_response("astream", "c1", "agroup")


if __name__ == '__main__':
    asyncio.run(main())


