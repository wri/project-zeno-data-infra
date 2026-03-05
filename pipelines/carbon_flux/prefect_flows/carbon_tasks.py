from typing import Optional, Tuple
import xarray as xr

from prefect import task
from pipelines.carbon_flux import stages

@task
def load_data(carbon_net_flux_zarr_uri,
              carbon_gross_removals_zarr_uri,
              carbon_gross_emissions_zarr_uri,
              tree_cover_density_2000_zarr_uri,
              mangrove_stock_2000_zarr_uri,
              tree_cover_gain_from_height_zarr_uri,
              ) -> Tuple[xr.DataArray, ...]:
    return stages.load_data(carbon_net_flux_zarr_uri,
                            carbon_gross_removals_zarr_uri,
                            carbon_gross_emissions_zarr_uri,
                            tree_cover_density_2000_zarr_uri,
                            mangrove_stock_2000_zarr_uri,
                            tree_cover_gain_from_height_zarr_uri,
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
