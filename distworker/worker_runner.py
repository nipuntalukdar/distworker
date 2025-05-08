import logging

from .worker import Worker

logger = logging.getLogger("worker_runner")


async def main(register_key: str = "workers", worker_id="worker1", consumer_id="c1"):
    logging.info("Starting worker")
    actor = None
    try:
        actor: Worker = await Worker.create(register_key=register_key, myid=worker_id)
        await actor.heart_beater()
        await actor.do_work(consumer_id)
    finally:
        if actor:
            await actor.do_cleanup()
