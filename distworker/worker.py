import asyncio
import logging
import secrets
import sys
import uuid
from concurrent.futures import Executor, ProcessPoolExecutor

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
        self._registry: str = register_key
        if not myid:
            self._myid = uuid.uuid4().hex
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.exception("Failed to start")
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
    ):
        execpool = pool
        if not execpool:
            execpool = ProcessPoolExecutor()

        redis_client = await RedisStream.create(redis_url, task_stream, task_group)
        await redis_client.add_to_hset(register_key, myid, get_packages_base64())
        return theclass(
            redis_client,
            redis_url,
            task_stream,
            task_group,
            register_key=register_key,
            pool=execpool,
            myid=myid,
        )

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
        asyncio.create_task(self.send_heartbeat(interval=10))

    async def send_heartbeat(self, interval=5):
        while True:
            try:
                await asyncio.sleep(interval)
                thistime = asyncio.get_event_loop().time()
                data = get_packages_base64(extrainfo={"time": thistime})
                success = await self._redis_stream.add_to_hset(
                    self._registry, self._myid, data
                )
                if not success:
                    logger.error("Failed to send heartbeat")
            except asyncio.CancelledError:
                logger.error("Heartbeat task cancelled")
            except Exception:
                logger.exception("error")

    async def __call__(self, work_func):
        if not self._pool:
            return self.noexecutor(work_func)
        else:
            logger.debug("Run in  pool")
            return await self._loop.run_in_executor(
                self._pool, self.noexecutor, work_func
            )

    async def do_cleanup(self):
        logger.info(f"Unregistering myself {self._myid}")
        await self._redis_stream.del_hkey(self._registry, self._myid)

    async def do_work(self, consumer_id: str | None = None):
        my_id = consumer_id
        if not consumer_id:
            my_id = f"worker_{secrets.token_hex()}"

        while True:
            logger.debug("Getting work")
            await self._redis_stream.dequeue_work(self, my_id)
