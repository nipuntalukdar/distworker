import base64
import json
import logging
from typing import Any, Dict

import pkg_resources

logger = logging.getLogger("utils")


class RemoteException(Exception):
    def __init__(self, code, message, stack_trace, *args):
        self._code = code
        self._message = message
        self._custom_stack_strace = stack_trace
        super().__init__(code, message, stack_trace, *args)

    def __str__(self):
        return f"""RemoteException: {self._code}
        {self._message}
        {self._custom_stack_strace}"""

    __repr__ = __str__


def get_packages() -> Dict[str, str]:
    ret: Dict[str, str] = {}
    for pkg in pkg_resources.working_set:
        ret[pkg.key] = str(pkg.version)
    return ret


def get_packages_dict(inp: Dict[bytes, bytes]) -> Dict[str, Dict[str, str]]:
    try:
        ret = {}
        for workid, workerdetails in inp.items():
            workid = workid.decode("utf-8")
            workerdetails = workerdetails.decode("utf-8")
            data = base64.b64decode(workerdetails)
            data = json.loads(data)
            ret[workid] = data
        return ret
    except Exception:
        logger.exception("error get packages dict")
        return None


def get_packages_base64(extrainfo: Dict[str, Any] = None) -> str:
    pkgs = get_packages()
    data = {"pkgs": pkgs}
    if extrainfo:
        data.update(extrainfo)
    data = json.dumps(data)
    return base64.b64encode(data.encode("utf-8")).decode("utf-8")
