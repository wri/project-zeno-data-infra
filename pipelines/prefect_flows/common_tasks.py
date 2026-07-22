from typing import Optional, Tuple

import pandas as pd
import xarray as xr
from prefect import task

from pipelines.prefect_flows import common_stages


@task
def load_data(
    base_zarr_uri: str, contextual_uri: Optional[str] = None
) -> Tuple[xr.DataArray, ...]:
    return common_stages.load_data(base_zarr_uri, contextual_uri)


@task
def compute_zonal_stat(
    dataset: xr.DataArray,
    groupbys: Tuple[xr.DataArray, ...],
    expected_groups: Tuple,
    funcname: str,
    method: Optional[str] = None,
) -> xr.DataArray:
    """Do the reduction with the specified groupbys. funcname is the name of the
    reduction function. method is the flox reduction strategy (e.g. "cohorts"),
    None lets flox choose."""
    return common_stages.compute(dataset, groupbys, expected_groups, funcname, method)


@task
def postprocess_result(result: xr.DataArray) -> pd.DataFrame:
    return common_stages.create_result_dataframe(result)


@task
def save_result(result_df: pd.DataFrame, result_uri: str) -> str:
    return common_stages.save_results(result_df, result_uri)
