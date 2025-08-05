from typing import Optional, Tuple
import xarray as xr

from prefect import task
from pipelines.natural_lands import stages

@task
def setup_compute(
    datasets: Tuple[xr.Dataset, ...],
    expected_groups,
    contextual_name: Optional[str] = None,
) -> Tuple:
    return stages.setup_compute(datasets, expected_groups, contextual_name)

@task
def postprocess_result(result: xr.Dataset):
    return stages.create_result_dataframe(result)
