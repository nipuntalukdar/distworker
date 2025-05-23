import json
import logging
import os
from typing import Dict, List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel
from redis import Redis

from distworker.utils import get_packages_dict

logger = logging.getLogger("UtilityService")
api_config_file = os.getenv(
    "WORKER_CONFIG_FILE", "/usr/share/distworker/api_configs.json"
)

api_configs = {}
try:
    with open(api_config_file, "r") as fp:
        configs = json.load(fp)
        api_configs.update(configs)
except Exception:
    pass

app = FastAPI()
redis_client: Redis = Redis.from_url(
    f"redis://{api_configs.get('redis_host', '127.0.0.1')}:{
        api_configs.get('redis_port', 6379)
    }"
)


class ErrorResponse(BaseModel):
    detail: str


class WorkerOne(BaseModel):
    theartbeat: int
    packages: Dict[str, str]
    workerid: str


class Workers(BaseModel):
    workers: List[WorkerOne]


class Tasks(BaseModel):
    status: Optional[str] = "Could not get task stats"
    totaltasks: Optional[int] = -1
    pending: Optional[int] = 0
    lag: Optional[int] = 0
    incomplete: Optional[int] = 0


@app.get(
    "/workers",
    response_model=Workers,
    responses={500: {"model": ErrorResponse, "detail": "internal server error"}},
    summary="Get list of workers",
)
async def get_workers():
    try:
        workers = redis_client.hgetall("workers")
        ret_workers = []
        if workers:
            workers = get_packages_dict(workers)
            for workerid, workervalue in workers.items():
                theartbeat = workervalue["time"]
                pkgs = workervalue["pkgs"]
                ret_workers.append(
                    WorkerOne(theartbeat=theartbeat, packages=pkgs, workerid=workerid)
                )
        return Workers(workers=ret_workers)

    except Exception:
        logger.exception("Error in getting workers")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get(
    "/taskstats",
    response_model=Tasks,
    responses={500: {"model": ErrorResponse, "detail": "internal server error"}},
    summary="Get task stats",
)
async def get_pending_tasks(response: Response):
    try:
        tasks = redis_client.xinfo_groups(api_configs.get("taskstream", "tasks"))
        for task in tasks:
            if task["name"].decode("utf-8") != api_configs.get(
                "taskgroup", "taskgroup"
            ):
                continue
            rettask = Tasks(
                totaltasks=task["entries-read"],
                incomplete=task["pending"] + task["lag"],
                pending=task["pending"],
                lag=task["lag"],
                status="Success",
            )
            print(rettask)
            return rettask

    except Exception:
        logger.exception("Error in getting workers")
        raise HTTPException(status_code=500, detail="Internal server error")
    response.status_code = status.HTTP_404_NOT_FOUND
    return Tasks()


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=api_configs.get("listen_host", "127.0.0.1"),
        port=api_configs.get("listen_port", 8989),
    )
