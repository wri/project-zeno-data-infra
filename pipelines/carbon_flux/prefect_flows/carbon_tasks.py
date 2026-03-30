from typing import Optional, Tuple

import xarray as xr
from prefect import task

from pipelines.carbon_flux import stages
from pipelines.carbon_flux.create_zarr import create_zarrs as create_zarrs_func


@task
def create_zarrs(overwrite=False):
    return create_zarrs_func(overwrite=overwrite)


@task
def load_data(
    carbon_net_flux_zarr_uri,
    carbon_gross_removals_zarr_uri,
    carbon_gross_emissions_zarr_uri,
    tree_cover_density_2000_zarr_uri,
    mangrove_stock_2000_zarr_uri,
    tree_cover_gain_from_height_zarr_uri,
    group: Optional[str] = None,
) -> Tuple[xr.DataArray, ...]:
    return stages.load_data(
        carbon_net_flux_zarr_uri,
        carbon_gross_removals_zarr_uri,
        carbon_gross_emissions_zarr_uri,
        tree_cover_density_2000_zarr_uri,
        mangrove_stock_2000_zarr_uri,
        tree_cover_gain_from_height_zarr_uri,
        group=group,
    )


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
