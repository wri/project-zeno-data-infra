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
            "area_ha": Column("float64", Check.isin([2.5, 5.0])),
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
    assert set(result["aoi_id"]) == {"BRA", "BRA.7", "BRA.7.124", "BRA.7.125"}
    assert (result["aoi_type"] == "admin").all()
    assert not {"country", "region", "subregion"} & set(result.columns)

    # every admin level totals the same area (4 px x 2.5 ha)
    country_total = result[~result.aoi_id.str.contains(r"\.")]["area_ha"].sum()
    subregion_total = result[result.aoi_id.str.count(r"\.") == 2]["area_ha"].sum()
    assert country_total == subregion_total == 10.0

    # verify dates and confidence levels
    bra_by_date = dict(
        zip(
            result.loc[result.aoi_id == "BRA", "alert_date"],
            result.loc[result.aoi_id == "BRA", "alert_confidence"],
        )
    )
    assert bra_by_date[datetime.date(2023, 1, 1)] == "low"  # day 2923
    assert bra_by_date[datetime.date(2023, 2, 1)] == "high"  # day 2954
    assert bra_by_date[datetime.date(2023, 3, 19)] == "high"  # day 3000


@pytest.mark.slow
@pytest.mark.integration
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.integrated_alerts.stages._load_zarr")
def test_gadm_integrated_alerts_multi_admin_rollup(
    mock_load_zarr,
    mock_save_parquet,
    multi_admin_alerts_ds,
    multi_country_ds,
    multi_region_ds,
    multi_subregion_ds,
    pixel_area_ds,
):
    """Alerts across two countries with multiple regions/subregions roll up to
    every admin level, and area is conserved within each country across adm levels."""
    mock_load_zarr.side_effect = [
        multi_admin_alerts_ds,
        multi_country_ds,
        multi_region_ds,
        multi_subregion_ds,
        pixel_area_ds,
    ]

    with prefect_test_harness():
        integrated_alerts_area(
            integrated_alerts_zarr_uri="s3://dummy_zarr_uri",
            version="test_v1",
        )

    result = mock_save_parquet.call_args[0][0]

    # all three admin levels are emitted for both countries
    assert set(result["aoi_id"]) == {
        "BRA",
        "IDN",
        "BRA.1",
        "BRA.2",
        "IDN.1",
        "BRA.1.10",
        "BRA.2.20",
        "IDN.1.30",
    }
    assert (result["aoi_type"] == "admin").all()

    # verify area is correct per country across adm levels
    # BRA = 2px, IDN = 2px, each px = 2.5 ha
    for country, n_pixels in (("BRA", 2), ("IDN", 2)):
        prefix = f"{country}."
        country_total = result.loc[result.aoi_id == country, "area_ha"].sum()
        region_total = result.loc[
            result.aoi_id.str.startswith(prefix)
            & (result.aoi_id.str.count(r"\.") == 1),
            "area_ha",
        ].sum()
        subregion_total = result.loc[
            result.aoi_id.str.startswith(prefix)
            & (result.aoi_id.str.count(r"\.") == 2),
            "area_ha",
        ].sum()
        assert (
            country_total == region_total == subregion_total == n_pixels * 2.5
        )


@pytest.mark.slow
@pytest.mark.integration
@patch("pipelines.prefect_flows.common_stages._save_parquet")
@patch("pipelines.integrated_alerts.stages._load_zarr")
def test_gadm_integrated_alerts_filters_pixels_with_no_country(
    mock_load_zarr,
    mock_save_parquet,
    integrated_alerts_ds,
    ocean_country_ds,
    region_ds,
    subregion_ds,
    pixel_area_ds,
):
    """Pixels with no iso are dropped. When nothing remains, the flow still
    completes and saves an empty, correctly-shaped result"""
    mock_load_zarr.side_effect = [
        integrated_alerts_ds,
        ocean_country_ds,
        region_ds,
        subregion_ds,
        pixel_area_ds,
    ]

    with prefect_test_harness():
        result_uri = integrated_alerts_area(
            integrated_alerts_zarr_uri="s3://dummy_zarr_uri",
            version="test_v1",
        )

    assert result_uri.endswith("admin-integrated-alerts.parquet")
    mock_save_parquet.assert_called_once()

    result = mock_save_parquet.call_args[0][0]
    assert len(result) == 0
    assert {"aoi_id", "aoi_type"}.issubset(result.columns)
    assert not {"country", "region", "subregion"} & set(result.columns)
