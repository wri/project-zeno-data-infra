from typing import Optional, Tuple

import xarray as xr
from prefect import task

from pipelines.integrated_alerts import stages
from pipelines.globals import ANALYTICS_BUCKET

INTEGRATED_ALERTS_PREFIX = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/integrated-alerts"


@task
def load_data(
    zarr_uri: str, contextual_uri: Optional[str] = None
) -> Tuple[xr.DataArray, ...]:
    return stages.load_data(zarr_uri, contextual_uri)


@task
def setup_compute(
    datasets: Tuple[xr.DataArray, ...],
    expected_groups,
    contextual_name: Optional[str] = None,
) -> Tuple:
    return stages.setup_compute(datasets, expected_groups, contextual_name)


@task
def postprocess_result(result: xr.DataArray):
    return stages.create_result_dataframe(result)
