import dask.array as da
import geopandas as gpd
import numpy as np
import pytest
import xarray as xr
from shapely.geometry import box


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
                da.array([[[4, 4], [8, 8]]], dtype=np.int16),
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


class FakeQCRepository:
    def load(self, aoi_id=None, aoi_type=None):
        return gpd.GeoDataFrame(geometry=[box(0, 0, 1, 1)])


class FakeGoogleEarthEngineDatasetRepository:
    def load(self, dataset, geometry, like=None):
        if dataset == "loss":
            return xr.Dataset(
                data_vars={
                    "loss": (
                        ("y", "x"),
                        np.array([[1, 0], [1, 1]], dtype=np.uint8),
                    ),
                    "treecover2000": (
                        ("y", "x"),
                        np.array([[40, 20], [50, 31]], dtype=np.uint8),
                    ),
                }
            )
        if dataset == "tcl_drivers":
            return xr.Dataset(
                data_vars={
                    "classification": (
                        ("y", "x"),
                        np.array([[1, 2], [1, 3]], dtype=np.uint8),
                    )
                }
            )
        if dataset == "area":
            return xr.DataArray(
                np.array([[10000, 10000], [10000, 20000]], dtype=np.float32),
                dims=("y", "x"),
                name="area",
            )
        raise ValueError(f"Unknown dataset: {dataset}")
