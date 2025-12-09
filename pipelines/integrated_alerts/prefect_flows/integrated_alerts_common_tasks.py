from typing import Optional, Tuple
import xarray as xr

from prefect import task

from pipelines.integrated_alerts import stages

@task
def load_data(integrated_alerts_zarr_uri: str, contextual_uri: Optional[str] = None) -> Tuple[xr.DataArray, ...]:
    return stages.load_data(integrated_alerts_zarr_uri, contextual_uri)
    
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
