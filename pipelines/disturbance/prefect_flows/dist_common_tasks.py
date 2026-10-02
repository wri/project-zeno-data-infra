from typing import Optional, Tuple

import xarray as xr
from prefect import task

from pipelines.disturbance import stages
from pipelines.globals import ANALYTICS_BUCKET

DIST_PREFIX = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/dist-alerts"


# Inputs are cluster-persisted arrays: nothing to gain from Prefect caching them.
@task(persist_result=False)
def load_alert_pixels(
    dist_zarr_uri: str, contextual_name: Optional[str] = None
) -> Tuple[xr.DataArray, ...]:
    return stages.alert_pixel_inputs(dist_zarr_uri, contextual_name)


@task(persist_result=False)
def setup_compute(
    datasets: Tuple[xr.DataArray, ...],
    expected_groups,
    contextual_name: Optional[str] = None,
) -> Tuple:
    return stages.setup_compute(datasets, expected_groups, contextual_name)


@task(persist_result=False)
def postprocess_result(result: xr.DataArray):
    return stages.create_result_dataframe(result)
