from typing import Optional, Tuple
import xarray as xr

from prefect import task
from pipelines.tree_cover_loss import stages

@task
def load_data(tree_cover_loss_uri: str, pixel_area_uri: Optional[str] = None) -> Tuple[xr.DataArray, ...]:
    return stages.load_data(tree_cover_loss_uri, pixel_area_uri)


@task
def setup_compute(
    datasets: Tuple[xr.DataArray, ...],
    expected_groups,
    contextual_name: Optional[str] = None,
) -> Tuple:
    return stages.setup_compute(datasets, expected_groups, contextual_name)
