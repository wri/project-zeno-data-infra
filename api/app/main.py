import logging
import os
from contextlib import asynccontextmanager

import aioboto3
from dask.distributed import Client, LocalCluster
from fastapi import FastAPI, Request
from fastapi.exception_handlers import (
    request_validation_exception_handler,
)
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse
from pyinstrument import Profiler

from .routers import land_change

ANALYSES_TABLE_NAME = os.environ.get("ANALYSES_TABLE_NAME")


# Configure logging
def setup_logging():
    log_level = os.getenv("LOG_LEVEL", "DEBUG").upper()
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Basic configuration with StreamHandler (ECS captures stdout/stderr)
    logging.basicConfig(
        level=log_level, format=log_format, handlers=[logging.StreamHandler()]
    )

    if (
        os.getenv("LOG_FORMAT", "JSON").upper() == "JSON"
    ):  # set LOG_FORMAT to "TEXT" for default logging
        from pythonjsonlogger.json import JsonFormatter

        logging.getLogger("uvicorn").handlers.clear()
        logging.getLogger("uvicorn.access").handlers.clear()
        logging.getLogger("uvicorn.error").handlers.clear()

        json_handler = logging.StreamHandler()
        formatter = JsonFormatter(fmt="%(asctime)s %(name)s %(levelname)s %(message)s")
        json_handler.setFormatter(formatter)
        logging.root.handlers = [json_handler]
        logging.getLogger("uvicorn.access").handlers = [json_handler]


# Initialize logging
setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the dask cluster
    if not os.getenv("DASK_SCHEDULER_ADDRESS"):
        logging.warning(
            "DASK_SCHEDULER_ADDRESS not set, starting local cluster. "
            "This is the intended behavior for local development only."
        )
        cluster = LocalCluster(n_workers=8, threads_per_worker=2)
        os.environ["DASK_SCHEDULER_ADDRESS"] = cluster.scheduler_address

    app.state.dask_client = await Client(
        os.environ["DASK_SCHEDULER_ADDRESS"],
        asynchronous=True,
    )

    # Create an AWS Session and connections to DyamoDb and S3
    session = aioboto3.Session()
    async with session.client("s3", region_name="us-east-1") as s3_client:
        async with session.resource("dynamodb", region_name="us-east-1") as dynamo:
            app.state.dynamodb_table = await dynamo.Table(ANALYSES_TABLE_NAME)
            app.state.s3_client = s3_client
            yield


app = FastAPI(lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logging.warning(
        {
            "event": "request_validation_failure",
            "severity": "medium",
            "validation_inputs": exc.body,
            "validation_message": exc.errors(),
        }
    )
    return await request_validation_exception_handler(request, exc)


@app.middleware("http")
async def profile_request(request: Request, call_next):
    if request.headers.get("X-Profile") == "1":
        profiler = Profiler(async_mode="enabled")
        profiler.start()
        try:
            response = await call_next(request)
        finally:
            profiler.stop()
        return HTMLResponse(profiler.output_html())
    return await call_next(request)


app.include_router(land_change.router)


@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI!"}
