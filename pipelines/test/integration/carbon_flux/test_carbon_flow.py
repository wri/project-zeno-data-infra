from unittest.mock import patch

import pandas as pd
import pytest
from prefect.testing.utilities import prefect_test_harness

from pipelines.carbon_flux.prefect_flows.carbon_flow import gadm_carbon_flux
from pipelines.carbon_flux.stages import qc_against_validation_source
from pipelines.test.integration.carbon_flux.conftest import (
    FakeCarbonGEERepository,
    FakeCarbonQCRepository,
)


@pytest.mark.integration
@patch("pipelines.carbon_flux.stages.qc_against_validation_source", return_value=True)
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.carbon_flux.stages._load_zarr")
@patch("pipelines.carbon_flux.stages.create_zarrs")
def test_carbon_flow_with_mocked_data(
    mock_create_zarrs,
    mock_load_zarr,
    mock_save_parquet,
    mock_qc,
    carbon_net_flux_ds,
    carbon_gross_removals_ds,
    carbon_gross_emissions_ds,
    country_ds,
    region_ds,
    subregion_ds,
    mangrove_ds,
    tree_cover_gain_ds,
    tree_cover_density_ds,
):
    mock_create_zarrs.return_value = {
        "carbon_net_flux": "s3://mock/carbon_net_flux.zarr",
        "carbon_gross_removals": "s3://mock/carbon_gross_removals.zarr",
        "carbon_gross_emissions": "s3://mock/carbon_gross_emissions.zarr",
    }
    # Must match the call order in stages.load_data:
    # net_flux, gross_removals, gross_emissions, country, region, subregion,
    # mangrove, tree_cover_gain, tree_cover_density
    mock_load_zarr.side_effect = [
        carbon_net_flux_ds,
        carbon_gross_removals_ds,
        carbon_gross_emissions_ds,
        country_ds,
        region_ds,
        subregion_ds,
        mangrove_ds,
        tree_cover_gain_ds,
        tree_cover_density_ds,
    ]

    with prefect_test_harness():
        gadm_carbon_flux(version="test_version", overwrite=True)

    result_df = mock_save_parquet.call_args[0][0]

    expected_columns = {"aoi_id", "aoi_type", "tree_cover_density", "carbontype", "value"}
    assert set(result_df.columns) == expected_columns

    assert set(result_df["carbontype"].unique()).issubset(
        {"carbon_net_flux", "carbon_gross_removals", "carbon_gross_emissions"}
    )
    assert set(result_df["tree_cover_density"].unique()).issubset({30, 50, 75})
    assert result_df["value"].dtype.kind == "f"


def test_qc_against_validation_source_with_fake_gee_repo():
    # Matches FakeCarbonGEERepository expected totals exactly:
    #   emissions = 100.0 Mg CO2e, removals = 50.0 Mg CO2e
    result_df_matching = pd.DataFrame(
        {
            "aoi_id": ["AFG.1.1", "AFG.1.1"],
            "aoi_type": ["admin2", "admin2"],
            "tree_cover_density": [30, 30],
            "carbontype": ["carbon_gross_emissions", "carbon_gross_removals"],
            "value": [100.0, 50.0],
        }
    )
    assert (
        qc_against_validation_source(
            result_df_matching,
            qc_feature_repository=FakeCarbonQCRepository(),
            gee_repository=FakeCarbonGEERepository(),
        )
        is True
    )

    # Values diverge significantly from GEE validation (2x > 5% threshold) → fail
    result_df_diverged = pd.DataFrame(
        {
            "aoi_id": ["AFG.1.1", "AFG.1.1"],
            "aoi_type": ["admin2", "admin2"],
            "tree_cover_density": [30, 30],
            "carbontype": ["carbon_gross_emissions", "carbon_gross_removals"],
            "value": [200.0, 100.0],
        }
    )
    assert (
        qc_against_validation_source(
            result_df_diverged,
            qc_feature_repository=FakeCarbonQCRepository(),
            gee_repository=FakeCarbonGEERepository(),
        )
        is False
    )
