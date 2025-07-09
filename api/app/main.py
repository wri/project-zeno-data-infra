from contextlib import asynccontextmanager

from dask.distributed import LocalCluster
from fastapi import FastAPI

from api.app.routers import dist_alerts


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the dask cluster
    app.state.dask_cluster = LocalCluster(processes=False, asynchronous=True)
    yield
    # Release the resources
    close_call = app.state.dask_cluster.close()
    if close_call is not None:
        await close_call


app = FastAPI(lifespan=lifespan)
app.include_router(
    dist_alerts.router,
    prefix="/v0/land_change/dist_alerts",
    tags=["β Land Change"]
)


@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI!"}
