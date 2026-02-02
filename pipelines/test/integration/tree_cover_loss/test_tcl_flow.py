from unittest.mock import patch

import pytest

from pipelines.tree_cover_loss.prefect_flows.tcl_flow import umd_tree_cover_loss


@pytest.mark.integration
@pytest.mark.slow
@patch("pipelines.tree_cover_loss.stages._load_zarr")
def test_tcl_flow_with_new_contextual_layers(
    mock_load_zarr,
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

    result_df = umd_tree_cover_loss()

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

    print(f"\nTCL integration test passed")
    print(f"Columns: {result_df.columns.tolist()}")
    print(f"Shape: {result_df.shape}")
