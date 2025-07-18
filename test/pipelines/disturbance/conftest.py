import pytest
import numpy as np
import xarray as xr
import dask.array as da


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
