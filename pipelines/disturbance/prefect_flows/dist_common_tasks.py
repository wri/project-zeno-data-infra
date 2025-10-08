from typing import Optional, Tuple

import xarray as xr
from prefect import task

from pipelines.disturbance import stages
from pipelines.disturbance.stages import ExpectedGroupsType


@task
def load_data(
    dist_zarr_uri: str, contextual_uri: Optional[str] = None
) -> Tuple[
    xr.Dataset, xr.Dataset, xr.Dataset, xr.Dataset, xr.Dataset, xr.Dataset | None
]:
    return stages.load_data(dist_zarr_uri, contextual_uri)


@task
def setup_compute(
    datasets: Tuple[xr.DataArray, ...],
    expected_groups: Tuple,
    contextual_name: Optional[str] = None,
) -> Tuple[xr.DataArray, Tuple[xr.DataArray, ...], Optional[ExpectedGroupsType]]:
    return stages.setup_compute(datasets, expected_groups, contextual_name)


@task
def postprocess_result(result: xr.DataArray):
    return stages.create_result_dataframe(result)
