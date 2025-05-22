import asyncio
import json
import logging
import os

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
):
    logging.info("Starting worker")
    actor = None
    try:
        actor: Worker = await Worker.create(
            register_key=register_key, myid=worker_id, worker_config=worker_configs
        )
        await actor.heart_beater()
        await actor.do_work(consumer_id)
    finally:
        if actor:
            await actor.do_cleanup()
            await asyncio.sleep(10)
