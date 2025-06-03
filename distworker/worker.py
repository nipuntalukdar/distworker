import asyncio
import logging
import os
import sys
import time
import traceback
import uuid
from concurrent.futures import Executor, ProcessPoolExecutor
from random import randint
from typing import Any, Self, Union

from cachetools import TLRUCache

from .dumpload import DumpLoad
from .redis_stream import RedisStream
from .utils import RemoteException, get_packages_base64

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
        self._min_idle_time_pending_c_disappeared = 1000000
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
            self._min_idle_time_pending = max(
                worker_config.get(
                    "min_idle_time_pending_c_disappeared",
                    self._min_idle_time_pending_c_disappeared,
                ),
                self._min_idle_time_pending_c_disappeared,
            )
        self._func_cache_ttl = max(
            3600, int(worker_config.get("func_cache_ttl", 86400))
        )
        self._func_cache_size = min(
            100000, int(worker_config.get("func_cache_size", 10000))
        )
        self._func_cache = TLRUCache(self._func_cache_size, self.cache_ttu)
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
        pool: Union[Executor, str] = None,
        myid: str = None,
        consumer_id: str = None,
        register_key: str = "workers",
        worker_config: dict = None,
    ) -> Self:
        execpool = pool
        if not execpool:
            execpool = ProcessPoolExecutor()
        if not isinstance(execpool, str) and not isinstance(execpool, Executor):
            logger.error("Invalid value for pool")
            return None
        if isinstance(execpool, str) and execpool != "local":
            logger.error("Incorrect type or value")
            return None

        logger.debug(f"Redis {redis_url}")
        redis_client: RedisStream = await RedisStream.create(
            redis_url, task_stream, task_group
        )
        if not redis_client:
            logger.error("Unable to get redis client")
            return None
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

    def cache_ttu(self, key, value, time_value):
        logger.debug(f"Called ttu for {key}, {time_value} {self._func_cache_ttl}")
        return time_value + self._func_cache_ttl

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
    def noexecutor(func, serialized_args, func_serialized=True):
        try:
            args, kwargs = DumpLoad.load_args(serialized_args)
            if func_serialized:
                func = DumpLoad.loadfn(func)
            if not func:
                return False, "NOTOK", None
            return_data = func(*args, **kwargs)
            return return_data, "OK", None
        except Exception as e:
            logger.exception("Execution failed")
            traceback_str = traceback.format_exc(limit=10)
            re = RemoteException(type(e), e.__str__(), traceback_str)
            exception_bytes = DumpLoad.dump(re)
            return None, "NOTOK", exception_bytes

    def is_in_process(self, message_id):
        return message_id in self._in_process_messages

    async def queue_response(
        self,
        result: Union[Any, None],
        ok: str,
        exception_str: Union[str, None],
        replystream: str,
        local_id: str,
        message_id: str,
    ):
        exception = False if not exception_str else True
        logger.debug(f"Result={result}, ok={ok}, exception={exception}")
        await self._response_queue.put(
            (result, ok, exception_str, replystream, local_id, message_id)
        )

    async def get_response(self):
        while self._keep_running:
            task, replystream, local_id, message_id = await self._queue.get()
            result, ok, exception_str = await task
            await self.queue_response(
                result, ok, exception_str, replystream, local_id, message_id
            )
            self._queue.task_done()
            self._current_tasks -= 1
            self._in_process_messages.discard(message_id)

    async def pending_processing_task(self):
        logger.debug("Pending work processor started")
        while self._keep_running:
            await asyncio.sleep(randint(60, 180))
            logger.debug("Trying to get pending work")
            max_work = self._max_tasks - self._current_tasks
            # roughly every 10th time, try to fetch messages pending for
            # very long time, because the corresponding consumer might have
            # been deleted
            consumer_id = self._consumer_id if randint(1, 10) != 10 else None
            if consumer_id:
                min_pending_time = self._min_idle_time_pending
            else:
                min_pending_time = self._min_idle_time_pending_c_disappeared
            await self._redis_stream.get_pending_work(
                self, consumer_id, max_work, min_pending_time
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

    async def get_func_from_cache(self, func_cache_id: str) -> Union[str, None]:
        func_serialized = isinstance(self._pool, ProcessPoolExecutor)
        func = self._func_cache.get(func_cache_id)
        if func:
            logger.debug("Got function in local cache")
            return func
        else:
            func, ttl = await self._redis_stream.getval_expiry(func_cache_id)
            if not func:
                logger.error(f"Function with id {func_cache_id} not in cache")
                return None
            if not func_serialized:
                func = DumpLoad.loadfn(func)
            self._func_cache[func_cache_id] = func
            logger.debug("Got the function from remote cache")
            return func

    async def __call__(
        self, func, args, replystream, local_id, message_id, func_cache_id
    ):
        self._current_tasks += 1
        self._in_process_messages.add(message_id)
        func_serialized = (
            isinstance(self._pool, ProcessPoolExecutor) or not func_cache_id
        )
        func_unserialized = None
        if func_cache_id and not func:
            # The function must be in cache
            logger.debug(f"Loading function from cache for id {func_cache_id}")
            func = await self.get_func_from_cache(func_cache_id)
            if not func:
                logger.error(f"Function is not in cache id={func_cache_id}")
                re = RemoteException(
                    RemoteException,
                    "Function not in cache",
                    traceback.format_stack(10)[:-1],
                )
                await self.queue_response(
                    None, "NOTOK", DumpLoad.dump(re), replystream, local_id, message_id
                )
                self._current_tasks -= 1
                return

        elif func_cache_id and func:
            # even if the function is in cache, update the ttl
            if not await self._redis_stream.setval_expiry(
                func_cache_id, func, self._func_cache_ttl
            ):
                logger.error(
                    f"Function could not be added to cache, id={func_cache_id}"
                )
                re = RemoteException(
                    RemoteException,
                    "Function could not be added to cache",
                    traceback.format_stack(10)[:-1],
                )
                self._func_cache.pop(func_cache_id)
                await self.queue_response(
                    None, "NOTOK", DumpLoad.dump(re), replystream, local_id, message_id
                )
                self._current_tasks -= 1
                return
            if not func_serialized:
                # local cache can have the unserialized function
                func_unserialized = DumpLoad.loadfn(func)
                self._func_cache[func_cache_id] = func_unserialized
                func = func_unserialized
            else:
                self._func_cache[func_cache_id] = func
        elif not func_cache_id and not func:
            # Error, one of them must be there
            logger.error("Both function and function id missing from task")
            re = RemoteException(
                RemoteException,
                "Function and function cache id, both are missing",
                traceback.format_stack(10)[:-1],
            )
            await self.queue_response(
                None, "NOTOK", DumpLoad.dump(re), replystream, local_id, message_id
            )
            self._current_tasks -= 1
            return
        if self._pool == "local":
            logger.debug("Run in same thread")
            result, ok, exception_str = self.noexecutor(func, args, func_serialized)
            await self.queue_response(
                result, ok, exception_str, replystream, local_id, message_id
            )
            self._current_tasks -= 1
        else:
            logger.debug(f"Run in  pool {type(func)}")
            task = self._loop.run_in_executor(
                self._pool, self.noexecutor, func, args, func_serialized
            )
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
