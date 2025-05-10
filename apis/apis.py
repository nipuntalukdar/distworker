import logging
from typing import Dict, List

import redis
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from distworker.utils import get_packages_dict

logger = logging.getLogger("UtilityService")

app = FastAPI()
redis_client = redis.Redis()


class ErrorResponse(BaseModel):
    detail: str


class WorkerOne(BaseModel):
    theartbeat: int
    packages: Dict[str, str]
    workerid: str


class Workers(BaseModel):
    workers: List[WorkerOne]


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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8989)
