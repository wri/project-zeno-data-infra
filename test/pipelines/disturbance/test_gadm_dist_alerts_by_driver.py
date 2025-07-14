import numpy as np
import pytest
import xarray as xr
from pandera.pandas import DataFrameSchema, Column, Check
from pipelines.disturbance.gadm_dist_alerts_by_driver import gadm_dist_alerts_by_driver


@pytest.fixture
def expected_groups_driver():
    return (
        # Match expected groups to minimal data values
        [12],  # Country values in minimal data
        [7],  # Region values
        [124, 125],  # Subregion values
        np.arange(5), # all the driver categories
        [731, 750, 800],  # Alert date values in minimal data
        [2, 3],  # Confidence values in minimal data
    )


@pytest.fixture
def mock_loader_driver():
    def _loader(dist_zarr_uri: str):
        # 1. dist_alerts dataset
        confidence_data = np.array([[[3, 2], [2, 3]]], dtype=np.int16)
        alert_date_data = np.array([[[750, 731], [731, 800]]], dtype=np.int16)
        dist_alerts = xr.Dataset(
            data_vars={
                "confidence": (("band", "y", "x"), confidence_data),
                "alert_date": (("band", "y", "x"), alert_date_data),
            },
            coords={
                "band": ("band", [1], {}),
                "y": ("y", [60.0, 59.99975], {}),
                "x": ("x", [-180.0, -179.99975], {}),
                "spatial_ref": ((), 0, {}),
            },
            attrs={},
        )

        # 2. GADM datasets
        country = xr.Dataset(
            data_vars={
                "band_data": (
                    ("band", "y", "x"),
                    np.array([[[12, 12], [12, 12]]], dtype=np.uint16),
                )
            },
        )

        region = xr.Dataset(
            data_vars={
                "band_data": (
                    ("band", "y", "x"),
                    np.array([[[7, 7], [7, 7]]], dtype=np.uint16),
                )
            },
        )

        subregion = xr.Dataset(
            data_vars={
                "band_data": (
                    ("band", "y", "x"),
                    np.array([[[124, 124], [124, 125]]], dtype=np.uint16),
                )
            },
        )

        driver_data = np.array([[[2, 1], [2, 2]]], dtype=np.uint8)
        driver = xr.Dataset(
            data_vars={
                "driver": (("band", "y", "x"), driver_data),
            },
            coords={
                "band": ("band", [1], {}),
                "y": ("y", [60.0, 59.99975], {}),
                "x": ("x", [-180.0, -179.99975], {}),
                "spatial_ref": ((), 0, {}),
            },
            attrs={},
        )


        return dist_alerts, country, region, subregion, driver

    return _loader


def test_gadm_dist_alerts_by_driver_happy_path(
    mock_loader_driver, expected_groups_driver, spy_saver
):
    """Test full workflow with in-memory dependencies"""
    result_uri = gadm_dist_alerts_by_driver(
        dist_zarr_uri="s3://dummy_zarr_uri",
        dist_version="test_v1",
        loader=mock_loader_driver,
        groups=expected_groups_driver,
        saver=spy_saver,
        overwrite=True,
    )

    assert (
        result_uri
        == "s3://gfw-data-lake/umd_glad_dist_alerts/test_v1/tabular/epsg-4326/zonal_stats/dist_alerts_by_adm2_driver.parquet"
    )


def test_gadm_dist_alerts_by_driver_result(mock_loader_driver, expected_groups_driver, spy_saver):
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
                df.groupby(["country", "region", "subregion", "driver", "alert_date",
                    "confidence"
                ]).transform("nunique")
                == 2
            ),
            name="two_confidences_per_group",
            error="Each location-date must have exactly 2 confidence levels",
        ),
    )

    gadm_dist_alerts_by_driver(
        dist_zarr_uri="s3://dummy_zarr_uri",
        dist_version="test_v1",
        loader=mock_loader_driver,
        groups=expected_groups_driver,
        saver=spy_saver,
        overwrite=True,
    )

    # Verify
    result = spy_saver.saved_data
    print(f"\nGADM dist alerts result:\n{result}")
    alert_schema.validate(result)
