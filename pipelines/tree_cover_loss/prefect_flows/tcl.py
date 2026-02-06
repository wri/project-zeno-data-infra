import logging
from typing import Optional

import numpy as np
import pandas as pd
from shapely.geometry import Polygon

from pipelines.globals import (
    carbon_emissions_zarr_uri,
    ifl_intact_forest_lands_zarr_uri,
    pixel_area_zarr_uri,
    tree_cover_density_zarr_uri,
    tree_cover_loss_zarr_uri,
    umd_primary_forests_zarr_uri,
    wri_google_1km_drivers_zarr_uri,
)

# tcd threshold mapping
thresh_to_pct = {
    1: "10",
    2: "15",
    3: "20",
    4: "25",
    5: "30",
    6: "50",
    7: "75",
}


def umd_tree_cover_loss(tasks):
    if not tasks.qc_against_validation_source():
        raise AssertionError("TCL did not pass QC validation, stopping job")

    compute_tree_cover_loss(tasks)


def compute_tree_cover_loss(tasks, bbox: Optional[Polygon] = None):
    logging.getLogger("distributed.client").setLevel(logging.ERROR)
    contextual_column_name = "tree_cover_loss_year"
    funcname = "sum"

    expected_groups = (
        np.arange(1, 25),  # tcl years
        np.arange(1, 8),  # tcd threshold
        np.arange(0, 2),  # ifl
        np.arange(0, 8),  # drivers
        np.arange(0, 2),  # primary_forests
        np.arange(999),  # countries
        np.arange(1, 86),  # adm1s
        np.arange(1, 854),  # adm2s
    )

    datasets = tasks.load_data(
        tree_cover_loss_zarr_uri,
        pixel_area_uri=pixel_area_zarr_uri,
        carbon_emissions_uri=carbon_emissions_zarr_uri,
        tree_cover_density_uri=tree_cover_density_zarr_uri,
        ifl_uri=ifl_intact_forest_lands_zarr_uri,
        drivers_uri=wri_google_1km_drivers_zarr_uri,
        primary_forests_uri=umd_primary_forests_zarr_uri,
        bbox=bbox,
    )

    compute_input = tasks.setup_compute(datasets, expected_groups)
    result = tasks.compute_zonal_stat(*compute_input, funcname=funcname)

    result_df: pd.DataFrame = tasks.postprocess_result(result)

    # convert year values (1-24) to actual years (2001-2024)
    result_df[contextual_column_name] = result_df[contextual_column_name] + 2000

    # convert tcl thresholds to percentages
    result_df["canopy_cover"] = result_df["canopy_cover"].map(thresh_to_pct)

    # convert ifl to boolean
    result_df["is_intact_forest"] = result_df["is_intact_forest"].astype(bool)

    # convert driver codes to labels
    categoryid_to_driver = {
        1: "Permanent agriculture",
        2: "Hard commodities",
        3: "Shifting cultivation",
        4: "Logging",
        5: "Wildfire",
        6: "Settlements and infrastructure",
        7: "Other natural disturbances",
    }

    result_df["driver"] = result_df["driver"].map(categoryid_to_driver)

    # convert primary forest to boolean
    result_df["is_primary_forest"] = result_df["is_primary_forest"].astype(bool)

    # convert country codes
    from pipelines.prefect_flows.common_stages import numeric_to_alpha3

    result_df["country"] = result_df["country"].map(numeric_to_alpha3)
    result_df.dropna(subset=["country"], inplace=True)

    return result_df
