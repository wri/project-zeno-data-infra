import pytest
import numpy as np
import xarray as xr
import dask.array as da


@pytest.fixture
def expected_groups():
    return (
        # Match expected groups to minimal data values
        [76],  # Country values in minimal data
        [7],  # Region values
        [124, 125],  # Subregion values
        [2923, 2954, 3000], # Alert date values in minimal data (days since 2014-12-31):
        [2, 3],  # Confidence values in minimal data
    )


@pytest.fixture
def integrated_alerts_ds():
    confidence_data = da.array([[[3, 2], [2, 3]]], dtype=np.int16)
    alert_date_data = da.array([[[2954, 2923], [2923, 3000]]], dtype=np.int16)
    integrated_alerts = xr.Dataset(
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

    return integrated_alerts


@pytest.fixture
def country_ds():
    country = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[76, 76], [76, 76]]], dtype=np.uint16),
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
def multi_admin_alerts_ds():
    """Alerts spanning BRA and IDN with uniform confidence and date so
     the rollup exercised is purely spatial."""
    confidence_data = da.array([[[3, 3], [3, 3]]], dtype=np.int16)
    alert_date_data = da.array([[[2923, 2923], [2923, 2923]]], dtype=np.int16)
    integrated_alerts = xr.Dataset(
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

    return integrated_alerts


@pytest.fixture
def multi_country_ds():
    # 76 -> "BRA", 360 -> "IDN"
    country = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[76, 76], [360, 360]]], dtype=np.uint16),
            )
        }
    )

    return country


@pytest.fixture
def multi_region_ds():
    region = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[1, 2], [1, 1]]], dtype=np.uint16),
            )
        }
    )

    return region


@pytest.fixture
def multi_subregion_ds():
    subregion = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[10, 20], [30, 30]]], dtype=np.uint16),
            )
        }
    )

    return subregion


@pytest.fixture
def ocean_country_ds():
    # ISO -> 0 means should be dropped (no country)
    country = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[0, 0], [0, 0]]], dtype=np.uint16),
            )
        }
    )

    return country
