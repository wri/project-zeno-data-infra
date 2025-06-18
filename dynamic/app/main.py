from typing import Any, Dict
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel, Extra

from dask.distributed import Client, LocalCluster
from flox.xarray import xarray_reduce
import xarray as xr
import numpy as np
from shapely.geometry import shape
from rasterio import Affine
from rasterio.features import rasterize
from pydantic import BaseModel
from contextlib import asynccontextmanager
import pandas as pd

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

class AnalysisInput(BaseModel):
    geojson: Dict[str, Any]
    dataset: str

JULIAN_DATE_2015 = 2457033
JULIAN_DATE_DIST_OFFSET = 731

@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI!"}

# You can add your custom route here
@app.post("/analysis")
def analyze(data: AnalysisInput):
    # rasterize
    geom = shape(data.geojson)

    dist_obj_name = "s3://gfw-data-lake/umd_glad_dist_alerts/v20250510/raster/epsg-4326/zarr/date_conf.zarr"
    dist_alerts = xr.open_zarr(dist_obj_name)

    sliced = dist_alerts.sel(x=slice(geom.bounds[0],geom.bounds[2]), y=slice(geom.bounds[3],geom.bounds[1]),).squeeze("band")
    clipped = sliced.rio.clip([data.geojson])

    alerts_count = xarray_reduce(
        clipped.alert_date, 
        *(
            clipped.alert_date,
            clipped.confidence
        ),
        func='count',
        expected_groups=(
            np.arange(731, 1590),
            [1, 2, 3]
        )
    ).compute()
    alerts_count.name = 'alert_count'

    alerts_df = alerts_count.to_dataframe().drop("band", axis=1).drop("spatial_ref", axis=1).reset_index()
    alerts_df.confidence = alerts_df.confidence.map({2: 'low', 3: 'high'})
    alerts_df.alert_date = pd.to_datetime(alerts_df.alert_date + JULIAN_DATE_2015 + JULIAN_DATE_DIST_OFFSET, origin='julian', unit='D').dt.strftime('%Y-%m-%d')
    alerts_json = alerts_df[alerts_df.alert_count > 0].to_dict(orient="records")

    return {
        "data": alerts_json,
        "status": "success"
    }

API_URL="http:"

class StrictBaseModel(BaseModel):
    class Config:
        extra = Extra.forbid
        validate_assignment = True


class Response(StrictBaseModel):
    data: Any
    status: str = "success"


class DataMartResourceLink(StrictBaseModel):
    link: str


class DataMartResourceLinkResponse(Response):
    data: DataMartResourceLink

@app.post("/v0/land_change/dist_alerts/analytics",
          response_class=ORJSONResponse,
          response_model=DataMartResourceLinkResponse,
          tags=["Beta LandChange"],
          status_code=202)
def create():
    resource_id="my_fake_id"
    link = DataMartResourceLink(link=f"{API_URL}/v0/land_change/dist_alerts/analytics/{resource_id}")
    return DataMartResourceLinkResponse(data=link)
