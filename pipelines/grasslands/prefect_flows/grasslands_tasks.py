from typing import Optional, Tuple

import xarray as xr
from prefect import task

from pipelines.grasslands import stages
from pipelines.grasslands.stages import ExpectedGroupsType


@task
def load_data(
    dist_zarr_uri: str, contextual_uri: Optional[str] = None
) -> Tuple[xr.DataArray, ...]:
    return stages.load_data(dist_zarr_uri, contextual_uri)


@task
def setup_compute(
    datasets: Tuple[xr.DataArray, ...],
    expected_groups,
    contextual_name: Optional[str] = None,
) -> Tuple[
    xr.DataArray,
    Tuple[xr.DataArray, xr.DataArray, xr.DataArray],
    ExpectedGroupsType | None,
]:
    return stages.setup_compute(datasets, expected_groups, contextual_name)
