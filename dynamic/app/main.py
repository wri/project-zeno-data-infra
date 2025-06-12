from typing import Any, Dict, Optional
from fastapi import FastAPI
from dask.distributed import Client, LocalCluster
from flox.xarray import xarray_reduce
import xarray as xr
import numpy as np
from shapely.geometry import shape
from pydantic import BaseModel
from contextlib import asynccontextmanager
import pandas as pd
import duckdb

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the dask cluster
    app.state.dask_cluster = LocalCluster(processes=False, asynchronous=True)
    duckdb.query('''
    CREATE OR REPLACE SECRET secret (
        TYPE s3,
        PROVIDER credential_chain
    );
    ''')
    
    yield
    # Release the resources
    close_call = app.state.dask_cluster.close()
    if close_call is not None:
        await close_call

app = FastAPI(lifespan=lifespan)

class AnalysisInput(BaseModel):
    country: str = None
    region: int = None
    subregion: int = None
    geojson: Optional[Dict[str, Any]] = None
    dataset: str

JULIAN_DATE_2015 = 2457033
JULIAN_DATE_DIST_OFFSET = 731

@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI!"}

# You can add your custom route here
@app.post("/analysis")
async def analyze(data: AnalysisInput):
    if data.geojson is not None:
        alerts_json = await _otf(data.geojson)
    else:
        alerts_json = await _from_gadm(data.country, data.region, data.subregion)
    
    return {
        "data": alerts_json,
        "status": "success"
    }


async def _otf(geojson):
    # rasterize
        geom = shape(geojson)

        dist_obj_name = "s3://gfw-data-lake/umd_glad_dist_alerts/v20250510/raster/epsg-4326/zarr/date_conf.zarr"
        dist_alerts = xr.open_zarr(dist_obj_name)

        sliced = dist_alerts.sel(x=slice(geom.bounds[0],geom.bounds[2]), y=slice(geom.bounds[3],geom.bounds[1]),).squeeze("band")
        clipped = sliced.rio.clip([geojson]).persist()

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

        return alerts_json

async def _from_gadm(country, region, subregion):
     alerts_df = duckdb.query(f"""
        SELECT alert_date, alert_confidence, SUM(count) as count
        FROM 's3://gfw-data-lake/sbtn_natural_lands/zarr/dist_alerts_by_natural_lands_adm2.parquet'
        WHERE countries = '{country}' AND regions = '{region}' AND subregions = '{subregion}'
        GROUP BY alert_date, alert_confidence ORDER BY alert_date, alert_confidence
     """).df()
     alerts_json = alerts_df.to_dict(orient="records")
     return alerts_json