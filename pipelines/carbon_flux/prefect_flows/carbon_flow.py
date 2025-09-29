import logging

import numpy as np
from prefect import flow

from pipelines.carbon_flux.prefect_flows import carbon_tasks
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists


@flow(name="Natural lands area")
def gadm_carbon_flux(overwrite: bool = False) -> str:
    logging.getLogger("distributed.client").setLevel(logging.ERROR)  # or logging.ERROR

    carbon_net_flux_zarr_uri = "s3://gfw-data-lake/gfw_forest_carbon_net_flux/v20250430/raster/epsg-4326/zarr/Mg_CO2e.zarr/"
    carbon_gross_removals_zarr_uri = "s3://gfw-data-lake/gfw_forest_carbon_gross_removals/v20250416/raster/epsg-4326/zarr/Mg_CO2e.zarr/"
    carbon_gross_emissions_zarr_uri = "s3://gfw-data-lake/gfw_forest_carbon_gross_emissions/v20250430/raster/epsg-4326/zarr/Mg_CO2e.zarr/"

    mangrove_stock_2000_zarr_uri = "s3://gfw-data-lake/jpl_mangrove_aboveground_biomass_stock_2000/v201902/raster/epsg-4326/zarr/is_mangrove.zarr/"

    tree_cover_gain_from_height_zarr_uri = "s3://gfw-data-lake/umd_tree_cover_gain_from_height/v20240126/raster/epsg-4326/zarr/period.zarr/"
    tree_cover_density_2000_zarr_uri = "s3://gfw-data-lake/umd_tree_cover_density_2000/v1.8/raster/epsg-4326/zarr/threshold.zarr/"
    tree_cover_loss_zarr_uri = (
        "s3://gfw-data-lake/umd_tree_cover_loss/v1.12/raster/epsg-4326/zarr/year.zarr/"
    )

    result_uri = "s3://gfw-data-lake/gfw_forest_carbon_net_flux/v20250430/tabular/zonal_stats/gadm/gadm_adm2.parquet"
    funcname = "sum"

    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    # fmt: off
    expected_groups = (
        np.arange(999),     # country iso codes
        np.arange(1, 86),   # region codes
        np.arange(1, 854),  # subregion codes
        np.arange(101),     # tree cover density
        np.arange(30),      # tree cover loss year
        [0, 1],             # mangrove
        [0, 1, 2, 3, 4],    # tree cover gain from height - period
    )
    # fmt: on

    datasets = carbon_tasks.load_data.with_options(name="carbon-flux-load-data")(
        carbon_net_flux_zarr_uri,
        carbon_gross_removals_zarr_uri,
        carbon_gross_emissions_zarr_uri,
        tree_cover_density_2000_zarr_uri,
        tree_cover_loss_zarr_uri,
        mangrove_stock_2000_zarr_uri,
        tree_cover_gain_from_height_zarr_uri,
    )

    compute_input = carbon_tasks.setup_compute.with_options(
        name="carbon-flux-setup-compute"
    )(datasets, expected_groups)

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="carbon-flux-compute-zonal-stats"
    )(*compute_input, funcname=funcname)
    result_df = carbon_tasks.postprocess_result.with_options(
        name="carbon-flux-postprocess-result"
    )(result_dataset)
    result_uri = common_tasks.save_result.with_options(name="carbon-flux-save-result")(
        result_df, result_uri
    )
    return result_uri
