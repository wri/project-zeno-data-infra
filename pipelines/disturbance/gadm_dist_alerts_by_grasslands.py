import numpy as np
import xarray as xr
import pandas as pd
import logging
from flox.xarray import xarray_reduce
from flox import ReindexArrayType, ReindexStrategy

from .check_for_new_alerts import s3_object_exists
from ..globals import DATA_LAKE_BUCKET, country_zarr_uri, region_zarr_uri, subregion_zarr_uri

def gadm_dist_alerts_by_grasslands(zarr_uri: str, version: str, overwrite: bool) -> str:
    """Run DIST alerts analysis in grasslands using Dask to create parquet, upload to S3 and return URI."""

    results_key = f"umd_glad_dist_alerts/{version}/tabular/epsg-4326/zonal_stats/dist_alerts_by_adm2_driver.parquet"
    results_uri = f"s3://{DATA_LAKE_BUCKET}/{results_key}"

    logging.getLogger("distributed.client").setLevel(logging.ERROR)

    return "str"