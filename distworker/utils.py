import base64
import json
from typing import Any, Dict

import pkg_resources


def get_packages() -> Dict[str, str]:
    ret: Dict[str, str] = {}
    for pkg in pkg_resources.working_set:
        ret[pkg.key] = str(pkg.version)
    return ret


def get_packages_base64(extrainfo: Dict[str, Any] = None) -> str:
    pkgs = get_packages()
    data = {"pkgs": pkgs}
    if extrainfo:
        data.update(extrainfo)
    data = json.dumps(data)
    return base64.b64encode(data.encode("utf-8")).decode("utf-8")
