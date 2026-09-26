from typing import Optional, Tuple

import xarray as xr
from prefect import task
from shapely.geometry import Polygon

from pipelines.globals import ANALYTICS_BUCKET
from pipelines.integrated_alerts import stages

INTEGRATED_ALERTS_PREFIX = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/integrated-alerts"


@task
def load_data(
    zarr_uri: str, natural_lands_uri: str, bbox: Optional[Polygon] = None
) -> Tuple[xr.DataArray, ...]:
    return stages.load_data(zarr_uri, natural_lands_uri, bbox)


@task
def setup_compute(datasets: Tuple[xr.DataArray, ...], expected_groups) -> Tuple:
    return stages.setup_compute(datasets, expected_groups)


@task
def postprocess_result(result: xr.DataArray):
    return stages.create_result_dataframe(result)
