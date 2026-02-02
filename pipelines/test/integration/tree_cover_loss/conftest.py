import dask.array as da
import numpy as np
import pytest
import xarray as xr


@pytest.fixture
def tcl_ds():
    tcl = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[1, 1], [2, 2]]], dtype=np.uint8),
            )
        },
        coords={
            "band": np.array([0], dtype=np.int16),
            "y": np.array([1.0, 0.0], dtype=np.float64),
            "x": np.array([0.0, 1.0], dtype=np.float64),
        },
    )
    return tcl


@pytest.fixture
def pixel_area_ds():
    pixel_area = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[100.0, 150.0], [200.0, 250.0]]], dtype=np.float32),
            )
        },
        coords={
            "band": np.array([0], dtype=np.int16),
            "y": np.array([1.0, 0.0], dtype=np.float64),
            "x": np.array([0.0, 1.0], dtype=np.float64),
        },
    )
    return pixel_area


@pytest.fixture
def carbon_emissions_ds():
    carbon = xr.Dataset(
        data_vars={
            "carbon_emissions_MgCO2e": (
                ("band", "y", "x"),
                da.array([[[1000.0, 1500.0], [2000.0, 2500.0]]], dtype=np.float32),
            )
        },
        coords={
            "band": np.array([0], dtype=np.int16),
            "y": np.array([1.0, 0.0], dtype=np.float64),
            "x": np.array([0.0, 1.0], dtype=np.float64),
        },
    )
    return carbon


@pytest.fixture
def tcd_ds():
    tcd = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[1, 1], [2, 2]]], dtype=np.uint8),
            )
        },
        coords={
            "band": np.array([0], dtype=np.int16),
            "y": np.array([1.0, 0.0], dtype=np.float64),
            "x": np.array([0.0, 1.0], dtype=np.float64),
        },
    )
    return tcd


@pytest.fixture
def ifl_ds():
    ifl = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[0, 1], [0, 1]]], dtype=np.int16),
            )
        },
        coords={
            "band": np.array([0], dtype=np.int16),
            "y": np.array([1.0, 0.0], dtype=np.float64),
            "x": np.array([0.0, 1.0], dtype=np.float64),
        },
    )
    return ifl


@pytest.fixture
def drivers_ds():
    drivers = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[1, 1], [2, 2]]], dtype=np.int16),
            )
        },
        coords={
            "band": np.array([0], dtype=np.int16),
            "y": np.array([1.0, 0.0], dtype=np.float64),
            "x": np.array([0.0, 1.0], dtype=np.float64),
        },
    )
    return drivers


@pytest.fixture
def primary_forests_ds():
    primary_forests = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[0, 1], [0, 0]]], dtype=np.uint8),
            )
        },
        coords={
            "band": np.array([0], dtype=np.int16),
            "y": np.array([1.0, 0.0], dtype=np.float64),
            "x": np.array([0.0, 1.0], dtype=np.float64),
        },
    )
    return primary_forests


@pytest.fixture
def country_ds():
    country = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[1, 1], [2, 2]]], dtype=np.int16),
            )
        },
        coords={
            "band": np.array([0], dtype=np.int16),
            "y": np.array([1.0, 0.0], dtype=np.float64),
            "x": np.array([0.0, 1.0], dtype=np.float64),
        },
    )
    return country


@pytest.fixture
def region_ds():
    region = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[10, 10], [20, 20]]], dtype=np.uint8),
            )
        },
        coords={
            "band": np.array([0], dtype=np.int16),
            "y": np.array([1.0, 0.0], dtype=np.float64),
            "x": np.array([0.0, 1.0], dtype=np.float64),
        },
    )
    return region


@pytest.fixture
def subregion_ds():
    subregion = xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.array([[[100, 101], [200, 201]]], dtype=np.int16),
            )
        },
        coords={
            "band": np.array([0], dtype=np.int16),
            "y": np.array([1.0, 0.0], dtype=np.float64),
            "x": np.array([0.0, 1.0], dtype=np.float64),
        },
    )
    return subregion
