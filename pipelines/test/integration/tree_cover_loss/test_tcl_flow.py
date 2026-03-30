from unittest.mock import patch

import geopandas as gpd
import pytest
from prefect.testing.utilities import prefect_test_harness
from shapely.geometry import shape

from pipelines.test.integration.tree_cover_loss.conftest import (
    ARG_1_28,
)
from pipelines.tree_cover_loss.prefect_flows.tcl_flow import umd_tree_cover_loss_flow


@pytest.mark.integration
@pytest.mark.slow
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch(
    "pipelines.repositories.qc_feature_repository.QCFeaturesRepository.write_results"
)
@patch("pipelines.repositories.qc_feature_repository.QCFeaturesRepository.load")
def test_tcl_flow_real_data(mock_qc_load, mock_qc_write_results, mock_save_parquet):
    test_geom = shape(ARG_1_28)

    mock_qc_load.return_value = gpd.GeoDataFrame(
        {"geometry": [test_geom], "GID_2": "ARG.1.28_1"}
    )

    with prefect_test_harness():
        result_uri = umd_tree_cover_loss_flow(
            "v1.12", overwrite=True, bbox=test_geom.bounds
        )

    assert "admin-tree-cover-loss.parquet" in result_uri
    assert "v1.12" in result_uri

    # get the the saved df
    result_df = mock_save_parquet.call_args[0][0]

    # verify expected cols
    expected_columns = {
        "tree_cover_loss_year",
        "canopy_cover",
        "is_intact_forest",
        "driver",
        "is_primary_forest",
        "natural_forest_class",
        "aoi_id",
        "aoi_type",
        "area_ha",
        "carbon_Mg_CO2e",
        "tree_cover_loss_from_fires_area_ha",
    }
    assert set(result_df.columns) == expected_columns

    # verify dtypes
    assert result_df["is_intact_forest"].dtype == bool
    assert result_df["driver"].dtype == object
    assert result_df["is_primary_forest"].dtype == bool
    assert result_df["natural_forest_class"].dtype == object
    assert result_df.size == 24695
    mock_qc_write_results.assert_called_once()


@pytest.mark.integration
@patch(
    "pipelines.tree_cover_loss.stages.qc_against_validation_source", return_value=True
)
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.tree_cover_loss.stages._load_zarr")
def test_tcl_flow_with_new_contextual_layers(
    mock_load_zarr,
    mock_save_parquet,
    mock_qc,
    tcl_ds,
    pixel_area_ds,
    carbon_emissions_ds,
    tcd_ds,
    ifl_ds,
    drivers_ds,
    primary_forests_ds,
    natural_forests_ds,
    tclf_ds,
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
        natural_forests_ds,
        tclf_ds,
        country_ds,
        region_ds,
        subregion_ds,
    ]

    with prefect_test_harness():
        umd_tree_cover_loss_flow("test", overwrite=True)

    result_df = mock_save_parquet.call_args[0][0]

    # verify expected cols
    expected_columns = {
        "tree_cover_loss_year",
        "canopy_cover",
        "is_intact_forest",
        "driver",
        "is_primary_forest",
        "natural_forest_class",
        "aoi_id",
        "aoi_type",
        "area_ha",
        "carbon_Mg_CO2e",
        "tree_cover_loss_from_fires_area_ha",
    }
    assert set(result_df.columns) == expected_columns

    # verify dtypes
    assert result_df["is_intact_forest"].dtype == bool
    assert result_df["driver"].dtype == object
    assert result_df["is_primary_forest"].dtype == bool
    assert result_df["natural_forest_class"].dtype == object
    assert result_df.size == 33


@patch(
    "pipelines.tree_cover_loss.stages.qc_against_validation_source", return_value=True
)
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.tree_cover_loss.stages._load_zarr")
def test_tcl_flow_with_bbox(
    mock_load_zarr,
    mock_save_parquet,
    mock_qc,
    tcl_ds,
    pixel_area_ds,
    carbon_emissions_ds,
    tcd_ds,
    ifl_ds,
    drivers_ds,
    primary_forests_ds,
    natural_forests_ds,
    tclf_ds,
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
        natural_forests_ds,
        tclf_ds,
        country_ds,
        region_ds,
        subregion_ds,
    ]

    # filter to bottom left pixel
    with prefect_test_harness():
        umd_tree_cover_loss_flow("test", overwrite=True, bbox=(0, 1, 0, 1))

    result_df = mock_save_parquet.call_args[0][0]

    # verify expected cols
    expected_columns = {
        "tree_cover_loss_year",
        "canopy_cover",
        "is_intact_forest",
        "driver",
        "is_primary_forest",
        "natural_forest_class",
        "aoi_id",
        "aoi_type",
        "area_ha",
        "carbon_Mg_CO2e",
        "tree_cover_loss_from_fires_area_ha",
    }
    assert set(result_df.columns) == expected_columns

    # verify dtypes
    assert result_df["is_intact_forest"].dtype == bool
    assert result_df["driver"].dtype == object
    assert result_df["is_primary_forest"].dtype == bool
    assert result_df["natural_forest_class"].dtype == object
    assert result_df.size == 33
