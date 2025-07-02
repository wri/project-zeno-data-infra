from pandera.pandas import DataFrameSchema, Column, Check
from pipelines.disturbance.gadm_dist_alerts import gadm_dist_alerts


def test_gadm_dist_alerts_happy_path(mock_loader, expected_groups, spy_saver):
    """Test full workflow with in-memory dependencies"""
    result_uri = gadm_dist_alerts(
        dist_zarr_uri="s3://dummy_zarr_uri",
        dist_version="test_v1",
        loader=mock_loader,
        groups=expected_groups,
        saver=spy_saver,
    )

    assert (
        result_uri
        == "s3://gfw-data-lake/umd_glad_dist_alerts/test_v1/tabular/epsg-4326/zonal_stats/dist_alerts_by_adm2_raw_test.parquet"
    )


def test_gadm_dist_alerts_result(mock_loader, expected_groups, spy_saver):
    alert_schema = DataFrameSchema(
        name="GADM Dist Alerts",
        columns={
            "country": Column(int, Check.ge(0)),
            "region": Column(int, Check.ge(0)),
            "subregion": Column(int, Check.ge(0)),
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
        unique=["country", "region", "subregion", "alert_date", "confidence"],
        checks=Check(
            lambda df: (
                df.groupby(["country", "region", "subregion", "alert_date"])[
                    "confidence"
                ].transform("nunique")
                == 2
            ),
            name="two_confidences_per_group",
            error="Each location-date must have exactly 2 confidence levels",
        ),
    )

    gadm_dist_alerts(
        dist_zarr_uri="s3://dummy_zarr_uri",
        dist_version="test_v1",
        loader=mock_loader,
        groups=expected_groups,
        saver=spy_saver,
    )

    # Verify
    result = spy_saver.saved_data
    print(f"\nGADM dist alerts result:\n{result}")
    alert_schema.validate(result)
