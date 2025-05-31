import asyncio
import logging
import os

from distworker import worker_runner


async def main():
    thread_poopl = "local"
    worker_id = os.getenv("WORKER_ID", "worker1")
    consumer_id = os.getenv("CONSUMER_ID", "c1")
    await worker_runner.main(
        worker_id=worker_id, consumer_id=consumer_id, pool=thread_poopl
    )


if __name__ == "__main__":
    logging.getLogger("worker").info("Starting worker")
    asyncio.run(main())
