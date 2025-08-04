import pytest
import datetime

from unittest.mock import patch

from pandera.pandas import DataFrameSchema, Column, Check
from prefect.testing.utilities import prefect_test_harness

from pipelines.disturbance.prefect_flows import dist_alerts_count


@pytest.mark.slow
@pytest.mark.integration
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.prefect_flows.common_stages._load_zarr")
def test_gadm_dist_alerts_happy_path(
    mock_load_zarr, mock_save_parquet, dist_ds, country_ds, region_ds, subregion_ds
):
    """Test full workflow with in-memory dependencies"""

    mock_load_zarr.side_effect = [dist_ds, country_ds, region_ds, subregion_ds]

    with prefect_test_harness():
        result_uri = dist_alerts_count(
            dist_zarr_uri="s3://dummy_zarr_uri",
            dist_version="test_v1",
            # overwrite=True,
        )

    assert (
        result_uri
        == "s3://gfw-data-lake/umd_glad_dist_alerts/test_v1/tabular/statistics/admin.parquet"
    )


@pytest.mark.slow
@pytest.mark.integration
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.prefect_flows.common_stages._load_zarr")
def test_gadm_dist_alerts_result(
    mock_load_zarr, mock_save_parquet, dist_ds, country_ds, region_ds, subregion_ds
):
    alert_schema = DataFrameSchema(
        name="GADM Dist Alerts",
        columns={
            "country": Column(str, Check.ne("")),
            "region": Column(int, Check.ge(0)),
            "subregion": Column(int, Check.ge(0)),
            "alert_date": Column(
                datetime.date,
                checks=[
                    Check.greater_than_or_equal_to(datetime.date.fromisoformat("2023-01-01")),
                    Check.less_than_or_equal_to(datetime.date.fromisoformat("2023-03-11")),
                ],
            ),
            "alert_confidence": Column(str, Check.isin(["low", "high"])),
            "count": Column(int, Check.isin([0, 1, 2])),
        },
        unique=["country", "region", "subregion", "alert_date", "alert_confidence"],
        checks=Check(
            lambda df: (
                df.groupby(["country", "region", "subregion", "alert_date"])[
                    "alert_confidence"
                ].transform("nunique")
                == 1
            ),
            name="two_confidences_per_group",
            error="Each location-date must have exactly 1 confidence levels",
        ),
    )

    mock_load_zarr.side_effect = [dist_ds, country_ds, region_ds, subregion_ds]
    dist_alerts_count(dist_zarr_uri="s3://dummy_zarr_uri", dist_version="test_v1")

    # Verify
    result = mock_save_parquet.call_args[0][0]
    alert_schema.validate(result)
