from typing import Optional, Tuple
import xarray as xr

from prefect import task

from pipelines.disturbance import stages


@task
def setup_compute(
    datasets: Tuple[xr.Dataset, ...],
    expected_groups,
    contextual_name: Optional[str] = None,
) -> Tuple:
    return stages.setup_compute(datasets, expected_groups, contextual_name)
