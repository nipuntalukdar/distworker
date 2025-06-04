import json
import logging
import math
import random

from distworker.loop import distworkcache, start_loop

logger = logging.getLogger("decorator_test")


class TestCacheFun:
    def __init__(self):
        self._called = 0

    def __call__(self):
        logger.debug("Called cache decider")
        self._called += 1
        if self._called == 10:
            self._called = 0
            return True
        return False


cachedecider = TestCacheFun()


@distworkcache(cachename="cache1", cache_func=cachedecider, tasksqueue="tasks3")
def fun2(x, y):
    return x / y


@distworkcache(tasksqueue="tasks2", cachename="cache2")
def fun(x):
    x = abs(int(x))
    if x < 2:
        return f"Not prime {x}"
    if x <= 3:
        return f"Prime {x}"
    if x & 1 == 0:
        return f"Not prime {x}"
    this_end = int(math.sqrt(x))
    for i in range(3, this_end + 1, 2):
        if x % i == 0:
            return f"Not prime {x}"
    return f"Prime {x}"


configs = {}
with open("/usr/share/distworker/client_configs.json", "r") as fp:
    configs.update(json.load(fp))


start_loop(configs)
result_count = 0
exception_count = 0
for i in range(1, 201):
    try:
        logger.info("Submitting work")
        a = fun(i + random.randint(9999, 100000000))
        logger.info(f" ...................   Result is {a.result()}")
        result_count += 1
    except Exception:
        exception_count += 1
        logger.exception("Error")
    try:
        a = fun2(i, i % 2)
        logger.info(f" ...................   Result is {a.result()}")
        result_count += 1
    except Exception:
        exception_count += 1
        logger.exception("Error")
logger.info(f"Result={result_count}, expected=300")
logger.info(f"Exception={exception_count}, expected=100")
