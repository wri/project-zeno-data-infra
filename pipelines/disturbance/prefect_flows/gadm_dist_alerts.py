from typing import Tuple

import numpy as np
import pandas as pd
import xarray as xr

from prefect import flow, task


from pipelines.disturbance import stages


@task
def load_data(dist_zarr_uri: str):
    return stages.load_data(dist_zarr_uri)


@task
def setup_compute(datasets: Tuple[xr.Dataset, ...]) -> Tuple:
    expected_groups = (
        np.arange(894),  # country ISO codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.arange(731, 1590),  # dates values
        [1, 2, 3],  # confidence values
    )
    return stages.setup_compute(datasets, expected_groups)


@task
def compute_zonal_stat(dataset: xr.DataArray, groupbys: Tuple, expected_groups: Tuple):
    return stages.compute(dataset, groupbys, expected_groups)


@task
def postprocess_result(result: xr.Dataset):
    return stages.create_result_dataframe(result)


@task
def save_result(result_df: pd.DataFrame, dist_version: str):
    return stages.save_results(result_df, dist_version, "dist_alerts")


@flow(name="DIST alerts count")
def dist_alerts_count(dist_zarr_uri: str, dist_version: str):
    datasets = load_data(dist_zarr_uri)
    compute_input = setup_compute(datasets)

    result_dataset = compute_zonal_stat(*compute_input)
    result_df = postprocess_result(result_dataset)
    result_uri = save_result(result_df, dist_version)

    return result_uri
