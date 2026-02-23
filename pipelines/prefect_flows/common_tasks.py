import os
from typing import Callable, Optional, Tuple

import coiled
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
) -> xr.DataArray:
    """Do the reduction with the specified groupbys. funcname is the name of the
    reduction function"""
    return common_stages.compute(dataset, groupbys, expected_groups, funcname)


@task
def postprocess_result(result: xr.DataArray) -> pd.DataFrame:
    return common_stages.create_result_dataframe(result)


@task
def save_result(result_df: pd.DataFrame, result_uri: str) -> str:
    return common_stages.save_results(result_df, result_uri)


@task
def create_zarr(
    source_uri: str,
    zarr_uri: str,
    transform: Optional[Callable] = None,
    overwrite: bool = False,
    chunks: Optional[dict] = None,
) -> str:
    return common_stages.create_zarr(
        source_uri, zarr_uri, transform=transform, overwrite=overwrite, chunks=chunks
    )


@task
def create_cluster(
    name: str = "gnw_zonal_stat_count",
    n_workers: int = 10,
    max_workers: int = 50,
    scheduler_vm_types: list[str] = ["r7g.xlarge"],
    worker_vm_types: list[str] = ["r7g.2xlarge"],
):
    """Create a Coiled Dask cluster and return the client."""
    cluster = coiled.Cluster(
        name=name,
        region="us-east-1",
        n_workers=n_workers,
        tags={"project": name},
        scheduler_vm_types=scheduler_vm_types,
        worker_vm_types=worker_vm_types,
        compute_purchase_option="spot_with_fallback",
        no_client_timeout="5 seconds",
        container=os.getenv("PIPELINES_IMAGE"),
        environ={
            "AWS_REQUEST_PAYER": "requester",
        },
    )
    cluster.adapt(minimum=n_workers, maximum=max_workers)

    client = cluster.get_client()
    return client
