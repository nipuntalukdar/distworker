import logging
from typing import Callable

import cloudpickle

import distworker.configs.loggingconf as logconf

logger = logging.getLogger("dumpload")
logger.debug(f"LOG config {logconf.logg_conf_file()}")


class DumpLoad:
    @classmethod
    def dump(cls, data):
        return cloudpickle.dumps(data)

    @classmethod
    def load(cls, data):
        return cloudpickle.loads(data)

    @classmethod
    def dumpfn_args(cls, func: Callable = None, /, *args, **kwargs):
        dump_dict = {}
        if func:
            dump_dict["func"] = func
        if args:
            dump_dict["args"] = args
        if kwargs:
            dump_dict["kwargs"] = kwargs
        try:
            return cloudpickle.dumps(dump_dict)
        except Exception:
            logger.exception("dumpfn_args")
            return None

    @classmethod
    def dumpfn(cls, func: Callable):
        return cls.dump(func)

    @classmethod
    def dumpargs(cls, *args, **kwargs):
        dump_dict = {}
        if args:
            dump_dict["args"] = args
        if kwargs:
            dump_dict["kwargs"] = kwargs
        try:
            return cloudpickle.dumps(dump_dict)
        except Exception:
            logger.exception("dump_args")
            return None

    @classmethod
    def loadfn_args(cls, data: str):
        try:
            dump_dict = cloudpickle.loads(data)
            return (
                dump_dict.get("func", None),
                dump_dict.get("args", ()),
                dump_dict.get("kwargs", {}),
            )
        except Exception:
            logger.exception("loadfn_args")
            return None, None, None

    @classmethod
    def load_args(cls, data: str):
        try:
            dump_dict = cloudpickle.loads(data)
            return (
                dump_dict.get("args", ()),
                dump_dict.get("kwargs", {}),
            )
        except Exception:
            logger.exception("load_args")
            return None, None

    @classmethod
    def loadfn(cls, data: str):
        return cls.load(data)


if __name__ == "__main__":
    data = DumpLoad.dumpfn_args(lambda x, y: print(x, y), 1, 2)
    fun, args, kwargs = DumpLoad.loadfn_args(data)
    if not fun:
        exit(1)
    fun(*args, **kwargs)
