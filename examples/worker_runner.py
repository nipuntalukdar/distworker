import asyncio
import logging

from distworker import worker_runner


async def main():
    await worker_runner.main()


if __name__ == "__main__":
    logging.getLogger("worker").info("Starting worker")
    asyncio.run(main())
