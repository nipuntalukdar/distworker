import asyncio
import secrets
from concurrent.futures import Executor, ProcessPoolExecutor
import logging

import distworker.configs.loggingconf
from .dumpload import DumpLoad
from .redis_stream import RedisStream

logger = logging.getLogger("worker")

class Worker:
    def __init__(
        self,
        redisclient,
        redis_url: str = "redis://127.0.0.1:6379",
        task_stream: str = "tasks",
        task_group: str = "taskgroup",
        pool: Executor = None 
    ):
        self._redis_url = redis_url
        self._task_stream = task_stream
        self._task_group = task_group
        self._redis_stream = redisclient
        self._pool = pool
        self._loop = None
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            pass

    @classmethod
    async def create(
        theclass,
        redis_url: str = "redis://127.0.0.1:6379",
        task_stream: str = "tasks",
        task_group: str = "taskgroup",
        pool: Executor = None
    ):
        execpool = pool
        if not execpool:
            execpool = ProcessPoolExecutor()

        redis_client = await RedisStream.create(redis_url, task_stream, task_group)
        return theclass(redis_client, redis_url, task_stream, task_group, execpool)
    
    @staticmethod
    def noexecutor(work_func_task):
        try:
            func, args, kwargs = DumpLoad.loadfn(work_func_task)
            if not func:
                return False, "NOTOK"
            return_data = func(*args, **kwargs)
            return return_data, "OK"
        except:
            return False, "NOTOK"

    async def __call__(self, work_func):
        if not self._pool:
            return self.noexecutor(work_func)
        else:
            logger.debug("Run in  pool")
            return await self._loop.run_in_executor(self._pool, self.noexecutor, work_func)
             

    async def do_work(self, consumer_id: str | None = None):
        my_id = consumer_id
        if not consumer_id:
            my_id = f"worker_{secrets.token_hex()}"

        while True:
            logger.debug("Getting work")
            await self._redis_stream.dequeue_work(self, my_id)

