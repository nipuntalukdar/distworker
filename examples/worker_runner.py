import asyncio
import logging

from distworker import worker


async def main():
    actor = await worker.Worker.create()
    await actor.do_work("c1")


if __name__ == "__main__":
    logging.getLogger("worker").info("Starting worker")
    asyncio.run(main())
