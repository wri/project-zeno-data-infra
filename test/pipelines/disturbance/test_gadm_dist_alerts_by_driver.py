import pytest
from unittest.mock import patch

from pandera.pandas import DataFrameSchema, Column, Check
from prefect.testing.utilities import prefect_test_harness

from pipelines.disturbance.prefect_flows import dist_alerts_by_drivers_count


@pytest.mark.integration
@pytest.mark.slow
@patch("pipelines.disturbance.stages._save_parquet")
@patch("pipelines.disturbance.stages._load_zarr")
def test_gadm_dist_alerts_by_driver_happy_path(
    mock_load_zarr,
    mock_save_parquet,
    dist_ds,
    country_ds,
    region_ds,
    subregion_ds,
    dist_drivers_ds,
):
    """Test full workflow with in-memory dependencies"""

    mock_load_zarr.side_effect = [
        dist_ds,
        country_ds,
        region_ds,
        subregion_ds,
        dist_drivers_ds,
    ]

    with prefect_test_harness():
        result_uri = dist_alerts_by_drivers_count(
            dist_zarr_uri="s3://dummy_zarr_uri",
            dist_version="test_v1",
        )

    assert (
        result_uri
        == "s3://gfw-data-lake/umd_glad_dist_alerts/test_v1/tabular/zonal_stats/gadm/gadm_adm2_dist_alerts_by_drivers.parquet"
    )


@pytest.mark.slow
@pytest.mark.integration
@patch("pipelines.disturbance.stages._save_parquet")
@patch("pipelines.disturbance.stages._load_zarr")
def test_gadm_dist_alerts_by_driver_result(
    mock_load_zarr,
    mock_save_parquet,
    dist_ds,
    country_ds,
    region_ds,
    subregion_ds,
    dist_drivers_ds,
):
    alert_schema = DataFrameSchema(
        name="GADM Dist Alerts",
        columns={
            "country": Column(int, Check.ge(0)),
            "region": Column(int, Check.ge(0)),
            "subregion": Column(int, Check.ge(0)),
            "driver": Column(int, Check.ge(0)),
            "alert_date": Column(
                int,
                checks=[
                    Check.greater_than_or_equal_to(731),
                    Check.less_than_or_equal_to(800),
                ],
            ),
            "confidence": Column(int, Check.isin([2, 3])),
            "value": Column(int, Check.isin([0, 1, 2])),
        },
        unique=["country", "region", "subregion", "driver", "alert_date", "confidence"],
        checks=Check(
            lambda df: (
                df.groupby(["country", "region", "subregion", "driver", "alert_date"])[
                    "confidence"
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
        dist_drivers_ds,
    ]

    with prefect_test_harness():
        dist_alerts_by_drivers_count(
            dist_zarr_uri="s3://dummy_zarr_uri",
            dist_version="test_v1",
        )

    # Verify
    result = mock_save_parquet.call_args[0][0]
    print(f"\nGADM dist alerts result:\n{result}")
    alert_schema.validate(result)
