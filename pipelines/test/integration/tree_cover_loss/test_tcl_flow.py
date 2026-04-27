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
@patch("pipelines.tree_cover_loss.prefect_flows.tcl_tasks.create_zarrs_func")
def test_tcl_flow_real_data(
    mock_create_zarrs, mock_qc_load, mock_qc_write_results, mock_save_parquet
):
    test_geom = shape(ARG_1_28)

    mock_create_zarrs.return_value = {
        "tree_cover_loss": "s3://lcl-analytics/zarr/umd-tree-cover-loss/v1.12/year.zarr",
        "tree_cover_loss_from_fires": "s3://lcl-analytics/zarr/umd-tree-cover-loss-from-fires/v1.12/year.zarr",
        "drivers": "s3://lcl-analytics/zarr/wri-google-tree-cover-loss-drivers/v1.12/category.zarr",
    }
    mock_qc_load.return_value = gpd.GeoDataFrame(
        {"geometry": [test_geom], "GID_2": "ARG.1.28_1"}
    )

    with prefect_test_harness():
        result_uri = umd_tree_cover_loss_flow(
            "v1.12", overwrite=True, bbox=test_geom.bounds
        )

    assert "admin-tree-cover-loss" in result_uri
    assert "v1.12" in result_uri

    # get the the saved df
    result_df = mock_save_parquet.call_args[0][0]

    # verify expected cols
    expected_columns = {
        "tree_cover_loss_year",
        "canopy_cover",
        "is_intact_forest",
        "tree_cover_loss_driver",
        "is_primary_forest",
        "natural_forest_class",
        "aoi_id",
        "aoi_type",
        "area_ha",
        "carbon_emissions_MgCO2e",
        "tree_cover_loss_from_fires_area_ha",
    }
    assert set(result_df.columns) == expected_columns

    # verify dtypes
    assert result_df["is_intact_forest"].dtype == bool
    assert result_df["tree_cover_loss_driver"].dtype == object
    assert result_df["is_primary_forest"].dtype == bool
    assert result_df["natural_forest_class"].dtype == object
    assert result_df.size == 37455
    mock_qc_write_results.assert_called_once()


@pytest.mark.integration
@patch(
    "pipelines.tree_cover_loss.stages.qc_against_validation_source", return_value=True
)
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.tree_cover_loss.stages._load_zarr")
@patch("pipelines.tree_cover_loss.prefect_flows.tcl_tasks.create_zarrs_func")
def test_tcl_flow_with_new_contextual_layers(
    mock_create_zarrs,
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
    mangrove_ds,
    gain_from_height_ds,
    country_ds,
    region_ds,
    subregion_ds,
):
    mock_create_zarrs.return_value = {
        "tree_cover_loss": "s3://mock/tcl.zarr",
        "tree_cover_loss_from_fires": "s3://mock/tclf.zarr",
        "drivers": "s3://mock/drivers.zarr",
    }
    # These datasets should be listed in the order they are loaded in
    # tree_cover_loss/stages.py:load_data()
    mock_load_zarr.side_effect = [
        tcl_ds,
        pixel_area_ds,
        carbon_emissions_ds,
        tclf_ds,
        tcd_ds,
        ifl_ds,
        drivers_ds,
        primary_forests_ds,
        natural_forests_ds,
        mangrove_ds,
        gain_from_height_ds,
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
        "tree_cover_loss_driver",
        "is_primary_forest",
        "natural_forest_class",
        "aoi_id",
        "aoi_type",
        "area_ha",
        "carbon_emissions_MgCO2e",
        "tree_cover_loss_from_fires_area_ha",
    }
    assert set(result_df.columns) == expected_columns

    # verify dtypes
    assert result_df["is_intact_forest"].dtype == bool
    assert result_df["tree_cover_loss_driver"].dtype == object
    assert result_df["is_primary_forest"].dtype == bool
    assert result_df["natural_forest_class"].dtype == object
    assert result_df.size == 396


@patch(
    "pipelines.tree_cover_loss.stages.qc_against_validation_source", return_value=True
)
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.tree_cover_loss.stages._load_zarr")
@patch("pipelines.tree_cover_loss.prefect_flows.tcl_tasks.create_zarrs_func")
def test_tcl_flow_with_bbox(
    mock_create_zarrs,
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
    mangrove_ds,
    gain_from_height_ds,
    country_ds,
    region_ds,
    subregion_ds,
):
    mock_create_zarrs.return_value = {
        "tree_cover_loss": "s3://mock/tcl.zarr",
        "tree_cover_loss_from_fires": "s3://mock/tclf.zarr",
        "drivers": "s3://mock/drivers.zarr",
    }
    # These datasets should be listed in the order they are loaded in
    # tree_cover_loss/stages.py:load_data()
    mock_load_zarr.side_effect = [
        tcl_ds,
        pixel_area_ds,
        carbon_emissions_ds,
        tclf_ds,
        tcd_ds,
        ifl_ds,
        drivers_ds,
        primary_forests_ds,
        natural_forests_ds,
        mangrove_ds,
        gain_from_height_ds,
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
        "tree_cover_loss_driver",
        "is_primary_forest",
        "natural_forest_class",
        "aoi_id",
        "aoi_type",
        "area_ha",
        "carbon_emissions_MgCO2e",
        "tree_cover_loss_from_fires_area_ha",
    }
    assert set(result_df.columns) == expected_columns

    # verify dtypes
    assert result_df["is_intact_forest"].dtype == bool
    assert result_df["tree_cover_loss_driver"].dtype == object
    assert result_df["is_primary_forest"].dtype == bool
    assert result_df["natural_forest_class"].dtype == object
    assert result_df.size == 99
