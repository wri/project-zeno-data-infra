import datetime
from unittest.mock import patch

import pytest
from pandera.pandas import Check, Column, DataFrameSchema
from prefect.testing.utilities import prefect_test_harness

from pipelines.disturbance.prefect_flows import dist_alerts_area


@pytest.mark.slow
@pytest.mark.integration
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.disturbance.stages._load_zarr")
def test_gadm_dist_alerts_happy_path(
    mock_load_zarr,
    mock_save_parquet,
    dist_ds,
    country_ds,
    region_ds,
    subregion_ds,
    pixel_area_ds,
):
    """Test full workflow with in-memory dependencies"""

    mock_load_zarr.side_effect = [
        dist_ds,
        country_ds,
        region_ds,
        subregion_ds,
        pixel_area_ds,
    ]

    with prefect_test_harness():
        result_uri = dist_alerts_area(
            dist_zarr_uri="s3://dummy_zarr_uri",
            dist_version="test_v1",
        )

    assert (
        result_uri == "s3://lcl-analytics/dist-alerts/test_v1/admin-dist-alerts.parquet"
    )


@pytest.mark.slow
@pytest.mark.integration
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.disturbance.stages._load_zarr")
def test_gadm_dist_alerts_result(
    mock_load_zarr,
    mock_save_parquet,
    dist_ds,
    country_ds,
    region_ds,
    subregion_ds,
    pixel_area_ds,
):
    alert_schema = DataFrameSchema(
        name="GADM Dist Alerts",
        columns={
            "country": Column(str, Check.ne("")),
            "region": Column(int, Check.ge(0)),
            "subregion": Column(int, Check.ge(0)),
            "dist_alert_date": Column(
                datetime.date,
                checks=[
                    Check.greater_than_or_equal_to(
                        datetime.date.fromisoformat("2023-01-01")
                    ),
                    Check.less_than_or_equal_to(
                        datetime.date.fromisoformat("2023-03-11")
                    ),
                ],
            ),
            "area_ha": Column("float32", Check.isin([1500.0, 750.0])),
            "dist_alert_confidence": Column(str, Check.isin(["low", "high"])),
        },
        unique=[
            "country",
            "region",
            "subregion",
            "dist_alert_date",
            "dist_alert_confidence",
        ],
        checks=Check(
            lambda df: (
                df.groupby(["country", "region", "subregion", "dist_alert_date"])[
                    "dist_alert_confidence"
                ].transform("nunique")
                == 1
            ),
            name="two_confidences_per_group",
            error="Each location-date must have exactly 1 confidence levels",
        ),
    )

    mock_load_zarr.side_effect = [
        dist_ds,
        country_ds,
        region_ds,
        subregion_ds,
        pixel_area_ds,
    ]
    dist_alerts_area(
        dist_zarr_uri="s3://dummy_zarr_uri",
        dist_version="test_v1",
    )

    # Verify
    result = mock_save_parquet.call_args[0][0]
    alert_schema.validate(result)
