import logging
from typing import Optional

import numpy as np
import pandas as pd
from shapely.geometry import Polygon


def carbon_flux(
    tasks, version: Optional[str] = None, bbox: Optional[Polygon] = None
):
    if not tasks.qc_against_validation_source(version=version):
        raise AssertionError("Carbon analysis did not pass QC validation, stopping job")

    return gadm_carbon_flux(tasks, bbox)


def gadm_carbon_flux(tasks, bbox: Optional[Polygon] = None):
    logging.getLogger("distributed.client").setLevel(logging.ERROR)
    funcname = "sum"

    carbon_net_flux_zarr_uri = "s3://lcl-analytics/zarr/gfw_forest_carbon_net_flux/v20250430/Mg_CO2e.zarr/"
    carbon_gross_removals_zarr_uri = "s3://lcl-analytics/zarr/gfw_forest_carbon_gross_removals/v20250416/Mg_CO2e.zarr/"
    carbon_gross_emissions_zarr_uri = "s3://lcl-analytics/zarr/gfw_forest_carbon_gross_emissions/v20250430/Mg_CO2e.zarr/"
    mangrove_stock_2000_zarr_uri = "s3://lcl-analytics/zarr/jpl_mangrove_aboveground_biomass_stock_2000/v201902/is_mangrove.zarr/"
    tree_cover_gain_from_height_zarr_uri = "s3://lcl-analytics/zarr/umd_tree_cover_gain_from_height/v20240126/period.zarr/"
    tree_cover_density_2000_zarr_uri = "s3://lcl-analytics/zarr/umd_tree_cover_density_2000/v1.8/threshold.zarr/"

    expected_groups = (
        np.arange(999),  # country iso codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.arange(8),  # tree cover density
        [0, 1],  # mangrove boolean
        [0, 1],  # tree cover gain from height boolean
    )

    datasets = tasks.load_data(
        carbon_net_flux_zarr_uri,
        carbon_gross_removals_zarr_uri,
        carbon_gross_emissions_zarr_uri,
        tree_cover_density_2000_zarr_uri,
        mangrove_stock_2000_zarr_uri,
        tree_cover_gain_from_height_zarr_uri,
        bbox=bbox,
    )

    compute_input = tasks.setup_compute(datasets, expected_groups)
    result = tasks.compute_zonal_stat(*compute_input, funcname=funcname)

    result_df: pd.DataFrame = tasks.postprocess_result(result)

    return result_df
