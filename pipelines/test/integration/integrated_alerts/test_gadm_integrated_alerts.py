import datetime
from unittest.mock import patch

import pytest
from pandera.pandas import Check, Column, DataFrameSchema
from prefect.testing.utilities import prefect_test_harness

from pipelines.integrated_alerts.prefect_flows.gadm_integrated_alerts import (
    integrated_alerts_area,
)


@pytest.mark.slow
@pytest.mark.integration
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.integrated_alerts.stages._load_zarr")
def test_gadm_integrated_alerts_happy_path(
    mock_load_zarr,
    mock_save_parquet,
    integrated_alerts_ds,
    country_ds,
    region_ds,
    subregion_ds,
    pixel_area_ds,
):
    """Test full workflow with in-memory dependencies"""

    mock_load_zarr.side_effect = [
        integrated_alerts_ds,
        country_ds,
        region_ds,
        subregion_ds,
        pixel_area_ds,
    ]

    with prefect_test_harness():
        result_uri = integrated_alerts_area(
            integrated_alerts_zarr_uri="s3://dummy_zarr_uri",
            version="test_v1",
        )

    assert (
        result_uri
        == "s3://lcl-analytics/zonal-statistics/integrated-alerts/test_v1/admin-integrated-alerts.parquet"
    )


@pytest.mark.slow
@pytest.mark.integration
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.integrated_alerts.stages._load_zarr")
def test_gadm_integrated_alerts_result(
    mock_load_zarr,
    mock_save_parquet,
    integrated_alerts_ds,
    country_ds,
    region_ds,
    subregion_ds,
    pixel_area_ds,
):
    alert_schema = DataFrameSchema(
        name="GADM Integrated Alerts",
        columns={
            "aoi_id": Column(str, Check.ne("")),
            "aoi_type": Column(str, Check.eq("admin")),
            "alert_date": Column(
                datetime.date,
                checks=[
                    Check.greater_than_or_equal_to(
                        datetime.date.fromisoformat("2023-01-01")
                    ),
                    Check.less_than_or_equal_to(
                        datetime.date.fromisoformat("2023-12-31")
                    ),
                ],
            ),
            "alert_confidence": Column(str, Check.isin(["low", "high"])),
            "area_ha": Column("float64", Check.isin([750.0, 1500.0])),
        },
        unique=[
            "aoi_id",
            "alert_date",
            "alert_confidence",
        ],
        checks=Check(
            lambda df: (
                df.groupby(["aoi_id", "alert_date"])["alert_confidence"].transform(
                    "nunique"
                )
                == 1
            ),
            name="two_confidence_per_group",
            error="Each aoi/date must have exactly 1 confidence level",
        ),
    )

    mock_load_zarr.side_effect = [
        integrated_alerts_ds,
        country_ds,
        region_ds,
        subregion_ds,
        pixel_area_ds,
    ]

    with prefect_test_harness():
        integrated_alerts_area(
            integrated_alerts_zarr_uri="s3://dummy_zarr_uri",
            version="test_v1",
        )

    # Verify
    result = mock_save_parquet.call_args[0][0]
    print(f"\nGADM integrated alerts result:\n{result}")
    alert_schema.validate(result)

    # ensure that the result uses the rolled up aoi_id
    assert set(result["aoi_id"]) == {"DZA", "DZA.7", "DZA.7.124", "DZA.7.125"}
    assert (result["aoi_type"] == "admin").all()
    assert not {"country", "region", "subregion"} & set(result.columns)

    # every admin level totals the same area (4 px x 750)
    country_total = result[~result.aoi_id.str.contains(r"\.")]["area_ha"].sum()
    subregion_total = result[result.aoi_id.str.count(r"\.") == 2]["area_ha"].sum()
    assert country_total == subregion_total == 3000.0
