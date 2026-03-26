from typing import Optional, Tuple

import pandas as pd
import xarray as xr
from prefect import task
from shapely import Polygon

from pipelines.prefect_flows.common_tasks import compute_zonal_stat
from pipelines.carbon_flux.stages import CarbonFluxTasks

_tasks = CarbonFluxTasks()


@task
def load_data(
    carbon_net_flux_zarr_uri: str,
    carbon_gross_removals_zarr_uri: str,
    carbon_gross_emissions_zarr_uri: str,
    tree_cover_density_2000_zarr_uri: str,
    mangrove_stock_2000_zarr_uri: str,
    tree_cover_gain_from_height_zarr_uri: str,
    bbox: Optional[Polygon] = None,
) -> Tuple:
    return _tasks.load_data(
        carbon_net_flux_zarr_uri,
        carbon_gross_removals_zarr_uri,
        carbon_gross_emissions_zarr_uri,
        tree_cover_density_2000_zarr_uri,
        mangrove_stock_2000_zarr_uri,
        tree_cover_gain_from_height_zarr_uri,
        bbox,
    )


@task
def setup_compute(
    datasets: Tuple,
    expected_groups,
    contextual_name: Optional[str] = None,
) -> Tuple:
    return _tasks.setup_compute(datasets, expected_groups)


@task
def postprocess_result(result: xr.DataArray) -> pd.DataFrame:
    return _tasks.create_result_dataframe(result)


@task
def qc_against_validation_source(result_df: pd.DataFrame, version: Optional[str] = None) -> bool:
    return _tasks.qc_against_validation_source(result_df=result_df, version=version)


class CarbonFluxPrefectTasks:
    load_data = load_data.with_options(name="carbon-flux-load-data")
    setup_compute = setup_compute.with_options(
        name="carbon-flux-setup-compute"
    )
    compute_zonal_stat = compute_zonal_stat.with_options(
        name="carbon-flux-compute-zonal-stats"
    )
    postprocess_result = postprocess_result.with_options(
        name="carbon-flux-postprocess-result"
    )
    qc_against_validation_source = qc_against_validation_source.with_options(
        name="carbon-flux-qc-validation"
    )
