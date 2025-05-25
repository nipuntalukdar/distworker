import asyncio
import logging
import os
import sys
import time
import uuid
from concurrent.futures import Executor, ProcessPoolExecutor
from random import randint
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
        consumer_id: str = None,
        worker_response_queue: asyncio.Queue = None,
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
        self._max_wait_milli_second = 10000
        self._max_tasks = os.cpu_count()
        self._min_idle_time_pending = 100000
        self._in_process_messages = set()
        self._consumer_id = consumer_id if consumer_id else self._myid
        if worker_config:
            self._max_tasks = min(
                worker_config.get("max_tasks_in_queue", self._max_tasks), 256
            )
            self._max_wait_milli_second = max(
                worker_config.get("max_wait_milli_second", 10000), 1000
            )
            self._min_idle_time_pending = max(
                worker_config.get("min_idle_time_pending", self._min_idle_time_pending),
                self._min_idle_time_pending,
            )
        self._queue = asyncio.Queue(self._max_tasks * 2)
        self._response_queue = asyncio.Queue(self._max_tasks * 2)
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
        consumer_id: str = None,
        register_key: str = "workers",
        worker_config: dict = None,
    ) -> Self:
        execpool = pool
        if not execpool:
            execpool = ProcessPoolExecutor()
        logger.debug(f"Redis {redis_url}")
        redis_client: RedisStream = await RedisStream.create(
            redis_url, task_stream, task_group
        )

        consumer_id = consumer_id if consumer_id else myid

        worker = theclass(
            redis_client,
            redis_url,
            task_stream,
            task_group,
            register_key=register_key,
            pool=execpool,
            myid=myid,
            consumer_id=consumer_id,
            worker_config=worker_config,
        )
        redis_client.set_response_queue(worker._response_queue)

        success = await Worker.register(
            redis_client, register_key, myid, Worker.get_worker_ttl(worker_config)
        )
        if not success:
            logger.critical("Failed to register")
            sys.exit(1)
        return worker

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

    def is_in_process(self, message_id):
        return message_id in self._in_process_messages

    async def get_response(self):
        while self._keep_running:
            task, replystream, local_id, message_id = await self._queue.get()
            result, ok = await task
            await self._response_queue.put(
                (result, ok, replystream, local_id, message_id)
            )
            self._queue.task_done()
            self._current_tasks -= 1
            self._in_process_messages.remove(message_id)

    async def pending_processing_task(self):
        logger.debug("Pending work processor started")
        while self._keep_running:
            await asyncio.sleep(randint(45, 180))
            logger.debug("Trying to get pending work")
            max_work = self._max_tasks - self._current_tasks
            await self._redis_stream.get_pending_work(
                self, self._consumer_id, max_work, self._min_idle_time_pending
            )

    async def process_pending(self):
        asyncio.create_task(self.pending_processing_task())

    async def response_task(self):
        for i in range(self._max_tasks):
            asyncio.create_task(self.get_response())
        for i in range(self._max_tasks):
            asyncio.create_task(self._redis_stream.response_processor())

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

    async def __call__(self, work_func, replystream, local_id, message_id):
        self._current_tasks += 1
        self._in_process_messages.add(message_id)
        ret_data = None
        task = None
        if not self._pool:
            logger.debug("Run in same thread")
            ret_data = self.noexecutor(work_func)
            self._current_tasks -= 1
            return ret_data
        else:
            logger.debug("Run in  pool")
            task = self._loop.run_in_executor(self._pool, self.noexecutor, work_func)
            await self._queue.put((task, replystream, local_id, message_id))

    async def do_cleanup(self):
        logger.info(f"Unregistering myself {self._myid}")
        await self._redis_stream.del_hkey(self._registry, self._myid)
        self._keep_running = False

    async def do_work(self):
        my_consumer_id = self._consumer_id
        diag_info_time = time.time()

        while self._keep_running:
            logger.debug("Getting work")
            if self._current_tasks >= self._max_tasks:
                if time.time() - diag_info_time > 2:
                    logger.debug("Completely occupied, not taking new tasks this time")
                    diag_info_time = time.time()
                await asyncio.sleep(2)
                continue
            try:
                await self._redis_stream.dequeue_work(
                    self,
                    my_consumer_id,
                    self._max_tasks - self._current_tasks,
                    self._max_wait_milli_second,
                )
            except asyncio.CancelledError:
                logger.error("Dequeue work stopping as cancelled")
                break
            except Exception:
                logger.exception("Error")
