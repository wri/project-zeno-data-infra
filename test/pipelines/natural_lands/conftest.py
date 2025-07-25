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
        [2, 2],  # Natural land values
    )


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
def area_ds():
    drivers = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[760.0, 760.0], [760.0, 760.0]]], dtype=np.float32),
            )
        },
    )

    return drivers
