from contextlib import asynccontextmanager

from dask.distributed import Client, LocalCluster
from fastapi import FastAPI

from .routers import land_change


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the dask cluster
    app.state.dask_cluster = LocalCluster(processes=False, asynchronous=True)
    app.state.dask_client = Client(app.state.dask_cluster)
    yield
    # Release the resources
    close_call = app.state.dask_cluster.close()
    if close_call is not None:
        await close_call


app = FastAPI(lifespan=lifespan)

app.include_router(land_change.router)


@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI!"}
