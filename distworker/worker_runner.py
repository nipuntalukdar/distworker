import asyncio
import json
import logging
import os
from concurrent.futures import Executor
from typing import Union

from .worker import Worker

logger = logging.getLogger("worker_runner")
worker_config_file = os.getenv(
    "WORKER_CONFIG_FILE", "/usr/share/distworker/worker_configs.json"
)

worker_configs = {}
try:
    with open(worker_config_file, "r") as fp:
        configs = json.load(fp)
        worker_configs.update(configs)
except Exception:
    pass


async def main(
    register_key: str = "workers",
    worker_id="worker1",
    consumer_id="c1",
    worker_config: dict = None,
    pool: Union[Executor, None] = None,
):
    logging.info("Starting worker")
    actor = None
    try:
        actor: Worker = await Worker.create(
            register_key=register_key,
            myid=worker_id,
            worker_config=worker_configs,
            consumer_id=consumer_id,
            pool=pool,
        )
        if actor:
            await actor.heart_beater()
            await actor.response_task()
            await actor.process_pending()
            await actor.do_work()
        else:
            logger.error("Startup error")
    finally:
        if actor:
            await actor.do_cleanup()
            await asyncio.sleep(10)
