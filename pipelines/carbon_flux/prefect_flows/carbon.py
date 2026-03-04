import logging
from typing import Optional

import numpy as np
import pandas as pd
from shapely.geometry import Polygon

from pipelines.prefect_flows.common_stages import numeric_to_alpha3

# tcd threshold mapping
thresh_to_pct = {
    0: 0,
    1: 10,
    2: 15,
    3: 20,
    4: 25,
    5: 30,
    6: 50,
    7: 75,
}


def gadm_carbon_flux(tasks, bbox: Optional[Polygon] = None):
    return compute_carbon_flux(tasks, bbox)


def compute_carbon_flux(tasks, bbox: Optional[Polygon] = None):
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

    # Convert tcd thresholds to percentages (already done in stages.py)
    # Convert country codes to alpha3
    result_df["country"] = result_df["country"].map(numeric_to_alpha3)
    result_df.dropna(subset=["country"], inplace=True)

    return result_df
