import asyncio
import logging
import os
import secrets
import sys
import time
import uuid
from concurrent.futures import Executor, ProcessPoolExecutor
from typing import Self

from .dumpload import DumpLoad
from .redis_stream import RedisStream
from .utils import get_packages_base64

logger = logging.getLogger("worker")


class Worker:
    def __init__(
        self,
        redisclient: RedisStream,
        redis_url: str = "redis://127.0.0.1:6379",
        task_stream: str = "tasks",
        task_group: str = "taskgroup",
        register_key: str = "workers",
        worker_config: dict = None,
        myid: str = None,
        pool: Executor = None,
    ):
        self._redis_url: str = redis_url
        self._task_stream: str = task_stream
        self._task_group: str = task_group
        self._redis_stream: RedisStream = redisclient
        self._pool: Executor = pool
        self._myid: str = myid
        self._loop = None
        self._keep_running = True
        self._current_tasks = 0
        self._max_tasks = os.cpu_count()
        if worker_config:
            self._max_tasks = worker_config.get("max_tasks_in_queue", self._max_tasks)
        self._registry: str = register_key
        self._worker_config: dict = worker_config
        if not myid:
            self._myid = uuid.uuid4().hex
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.exception("Failed to start")
            self._keep_running = False
            sys.exit(1)

    @classmethod
    async def create(
        theclass,
        redis_url: str = "redis://127.0.0.1:6379",
        task_stream: str = "tasks",
        task_group: str = "taskgroup",
        pool: Executor = None,
        myid: str = None,
        register_key: str = "workers",
        worker_config: dict = None,
    ) -> Self:
        execpool = pool
        if not execpool:
            execpool = ProcessPoolExecutor()

        redis_client: RedisStream = await RedisStream.create(
            redis_url, task_stream, task_group
        )
        success = await Worker.register(
            redis_client, register_key, myid, Worker.get_worker_ttl(worker_config)
        )
        if not success:
            logger.critical("Failed to register")
            sys.exit(1)

        return theclass(
            redis_client,
            redis_url,
            task_stream,
            task_group,
            register_key=register_key,
            pool=execpool,
            myid=myid,
            worker_config=worker_config,
        )

    @staticmethod
    async def register(redis_client, registry_key, myid, expiretime):
        thistime = int(time.time())
        data = get_packages_base64(extrainfo={"time": thistime})
        success = await redis_client.add_to_hset(registry_key, myid, data, expiretime)
        if not success:
            logger.error("Failed to send heartbeat")
            return False
        return True

    @staticmethod
    def noexecutor(work_func_task):
        try:
            func, args, kwargs = DumpLoad.loadfn(work_func_task)
            if not func:
                return False, "NOTOK"
            return_data = func(*args, **kwargs)
            return return_data, "OK"
        except Exception:
            logger.exception("execute")
            return False, "NOTOK"

    async def heart_beater(self):
        asyncio.create_task(self.send_heartbeat())

    @staticmethod
    def get_worker_ttl(worker_config: dict):
        interval = worker_config.get("heartbeat_interval", 10)
        return max(20, 6 * interval)

    async def send_heartbeat(self):
        interval = self._worker_config.get("heartbeat_interval", 10)
        next_heart_beat = time.time() + interval
        while True:
            try:
                sleep_time = max(0, next_heart_beat - time.time())
                next_heart_beat = time.time() + interval
                await asyncio.sleep(sleep_time)
                await Worker.register(
                    self._redis_stream,
                    self._registry,
                    self._myid,
                    self.get_worker_ttl(self._worker_config),
                )
            except asyncio.CancelledError:
                logger.error("Heartbeat task cancelled")
                break
            except Exception:
                logger.exception("Error")

    async def __call__(self, work_func):
        self._current_tasks += 1
        ret_data = None
        if not self._pool:
            logger.debug("Run in same thread")
            ret_data = self.noexecutor(work_func)
            self._current_tasks -= 1
        else:
            logger.debug("Run in  pool")
            ret_data = await self._loop.run_in_executor(
                self._pool, self.noexecutor, work_func
            )
            self._current_tasks -= 1
        return ret_data

    async def do_cleanup(self):
        logger.info(f"Unregistering myself {self._myid}")
        await self._redis_stream.del_hkey(self._registry, self._myid)
        self._keep_running = False

    async def do_work(self, consumer_id: str | None = None):
        my_id = consumer_id
        if not consumer_id:
            my_id = f"worker_{secrets.token_hex()}"

        while self._keep_running:
            logger.debug("Getting work")
            if self._current_tasks >= self._max_tasks:
                await asyncio.sleep(0.01)
                continue
            try:
                await self._redis_stream.dequeue_work(self, my_id)
            except asyncio.CancelledError:
                logger.error("Dequeue work stopping as cancelled")
                break
            except Exception:
                logger.exception("Error")
