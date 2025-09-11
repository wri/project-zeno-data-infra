import logging
import os
from contextlib import asynccontextmanager

from dask.distributed import Client, LocalCluster
from fastapi import FastAPI, Request
from fastapi.exception_handlers import (
    request_validation_exception_handler,
)
from fastapi.exceptions import RequestValidationError

from .routers import land_change


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
    app.state.dask_cluster = LocalCluster(processes=True, asynchronous=True)
    app.state.dask_client = Client(app.state.dask_cluster, asynchronous=True)
    yield
    # Release the resources
    close_call = app.state.dask_client.shutdown()
    if close_call is not None:
        await close_call


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


app.include_router(land_change.router)


@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI!"}
