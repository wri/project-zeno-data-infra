from typing import Optional, Tuple
import xarray as xr
import pandas as pd

from prefect import task

from pipelines.disturbance import stages


@task
def load_data(dist_zarr_uri: str, contextual_uri: Optional[str] = None):
    return stages.load_data(dist_zarr_uri, contextual_uri)


@task
def setup_compute(
    datasets: Tuple[xr.Dataset, ...],
    expected_groups,
    contextual_name: Optional[str] = None,
) -> Tuple:
    return stages.setup_compute(datasets, expected_groups, contextual_name)


@task
def compute_zonal_stat(dataset: xr.DataArray, groupbys: Tuple, expected_groups: Tuple):
    return stages.compute(dataset, groupbys, expected_groups)


@task
def postprocess_result(result: xr.Dataset):
    return stages.create_result_dataframe(result)


@task
def save_result(result_df: pd.DataFrame, result_uri: str) -> str:
    return stages.save_results(result_df, result_uri)
