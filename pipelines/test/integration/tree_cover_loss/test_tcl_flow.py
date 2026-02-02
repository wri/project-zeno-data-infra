from unittest.mock import patch

import pytest
from prefect.testing.utilities import prefect_test_harness

from pipelines.tree_cover_loss.prefect_flows.tcl_flow import umd_tree_cover_loss


@pytest.mark.integration
@pytest.mark.slow
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.tree_cover_loss.stages._load_zarr")
def test_tcl_flow(
    mock_load_zarr,
    mock_save_parquet,
    tcl_ds,
    pixel_area_ds,
    carbon_emissions_ds,
    tcd_ds,
    ifl_ds,
    drivers_ds,
    primary_forests_ds,
    country_ds,
    region_ds,
    subregion_ds,
):

    mock_load_zarr.side_effect = [
        tcl_ds,
        pixel_area_ds,
        carbon_emissions_ds,
        tcd_ds,
        ifl_ds,
        drivers_ds,
        primary_forests_ds,
        country_ds,
        region_ds,
        subregion_ds,
    ]

    with prefect_test_harness():
        result_uri = umd_tree_cover_loss(overwrite=True)

    # verify correct output URI
    assert "admin-tree-cover-loss-emissions-2001-2024.parquet" in result_uri

    # get the the saved df
    result_df = mock_save_parquet.call_args[0][0]

    # verify expected cols
    expected_columns = {
        "tree_cover_loss_year",
        "canopy_cover",
        "is_intact_forest",
        "driver",
        "is_primary_forest",
        "country",
        "region",
        "subregion",
        "area_ha",
        "carbon_Mg_CO2e",
    }
    assert set(result_df.columns) == expected_columns

    # verify dtypes
    assert result_df["is_intact_forest"].dtype == bool
    assert result_df["driver"].dtype == object
    assert result_df["is_primary_forest"].dtype == bool

    assert result_df.size == 40
