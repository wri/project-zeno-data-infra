import dask.array as da
import numpy as np
import pytest
import xarray as xr

from pipelines import globals as zarr_uris
from pipelines.disturbance import stages

DIST_ZARR_URI = "s3://dummy_zarr_uri"


@pytest.fixture(autouse=True)
def fresh_alert_pixels():
    stages.release_alert_pixels()
    yield
    stages.release_alert_pixels()


@pytest.fixture
def load_zarr_by_uri(
    dist_ds,
    country_ds,
    region_ds,
    subregion_ds,
    pixel_area_ds,
    natural_lands_ds,
    dist_drivers_ds,
    grasslands_ds,
    land_cover_ds,
):
    """side_effect for a mocked stages._load_zarr, returning each fixture by URI."""
    zarrs = {
        DIST_ZARR_URI: dist_ds,
        zarr_uris.country_zarr_uri: country_ds,
        zarr_uris.region_zarr_uri: region_ds,
        zarr_uris.subregion_zarr_uri: subregion_ds,
        zarr_uris.pixel_area_zarr_uri: pixel_area_ds,
        zarr_uris.sbtn_natural_lands_zarr_uri: natural_lands_ds,
        zarr_uris.dist_driver_zarr_uri: dist_drivers_ds,
        zarr_uris.grasslands_zarr_uri: grasslands_ds,
        zarr_uris.land_cover_zarr_uri: land_cover_ds,
    }
    return lambda uri: zarrs[uri]


@pytest.fixture
def expected_groups():
    return (
        # Match expected groups to minimal data values
        [12],  # Country values in minimal data
        [7],  # Region values
        [124, 125],  # Subregion values
        [731, 750, 800],  # Alert date values in minimal data
        [2, 3],  # Confidence values in minimal data
    )


@pytest.fixture
def dist_ds():
    confidence_data = da.array([[[3, 2], [2, 3]]], dtype=np.int16)
    alert_date_data = da.array([[[750, 731], [731, 800]]], dtype=np.int16)
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

    return dist_alerts


@pytest.fixture
def country_ds():
    country = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[12, 12], [12, 12]]], dtype=np.uint16),
            )
        }
    )

    return country


@pytest.fixture
def region_ds():
    region = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[7, 7], [7, 7]]], dtype=np.uint16),
            )
        }
    )
    return region


@pytest.fixture
def subregion_ds():
    subregion = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[124, 124], [124, 125]]], dtype=np.uint16),
            )
        }
    )

    return subregion


@pytest.fixture
def natural_lands_ds():
    natural_lands = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[2, 2], [2, 2]]], dtype=np.uint8),
            )
        },
    )

    return natural_lands


@pytest.fixture
def dist_drivers_ds():
    drivers = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[2, 2], [2, 2]]], dtype=np.uint8),
            )
        },
    )

    return drivers


@pytest.fixture
def pixel_area_ds():
    pixel_area = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[750.0, 750.0], [750.0, 750.0]]], dtype=np.float32),
            )
        },
    )

    return pixel_area


@pytest.fixture
def grasslands_ds():
    grasslands = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x", "year"),
                da.array([[[[1, 0], [0, 1]], [[1, 1], [1, 0]]]], dtype=np.uint8),
            )
        },
        coords={
            "band": ("band", [1], {}),
            "y": ("y", [60.0, 59.99975], {}),
            "x": ("x", [-180.0, -179.99975], {}),
            "year": ("year", [2021, 2022], {}),
        },
    )

    return grasslands


@pytest.fixture
def land_cover_ds():
    land_cover = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x", "year"),
                da.array([[[[3, 4], [6, 1]], [[0, 4], [7, 2]]]], dtype=np.uint8),
            )
        },
        coords={
            "band": ("band", [1], {}),
            "y": ("y", [60.0, 59.99975], {}),
            "x": ("x", [-180.0, -179.99975], {}),
            "year": ("year", [2023, 2024], {}),
        },
    )

    return land_cover
