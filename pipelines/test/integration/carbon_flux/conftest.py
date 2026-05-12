import dask.array as da
import geopandas as gpd
import numpy as np
import pytest
import xarray as xr
from shapely.geometry import box


def _make_band_dataset(values, dtype):
    return xr.Dataset(
        data_vars={
            "band_data": (
                ("band", "y", "x"),
                da.from_array(np.array(values, dtype=dtype)),
            )
        },
        coords={
            "band": np.array([0], dtype=np.int16),
            "y": np.array([1.0, 0.0], dtype=np.float64),
            "x": np.array([0.0, 1.0], dtype=np.float64),
        },
    )


@pytest.fixture
def carbon_net_flux_ds():
    return _make_band_dataset([[[1.0, -1.0], [-2.0, 2.0]]], np.float64)


@pytest.fixture
def carbon_gross_removals_ds():
    return _make_band_dataset([[[-5.0, -10.0], [-15.0, -20.0]]], np.float64)


@pytest.fixture
def carbon_gross_emissions_ds():
    return _make_band_dataset([[[10.0, 20.0], [30.0, 40.0]]], np.float64)


@pytest.fixture
def tree_cover_density_ds():
    # Values 5, 6, 7 map to 30%, 50%, 75%; 0 maps to <30%
    return _make_band_dataset([[[5, 6], [7, 0]]], np.uint8)


@pytest.fixture
def mangrove_ds():
    return _make_band_dataset([[[0, 1], [0, 0]]], np.uint8)


@pytest.fixture
def tree_cover_gain_ds():
    return _make_band_dataset([[[0, 0], [1, 0]]], np.uint8)


@pytest.fixture
def country_ds():
    return _make_band_dataset([[[4, 4], [4, 4]]], np.int16)


@pytest.fixture
def region_ds():
    return _make_band_dataset([[[1, 1], [1, 1]]], np.uint8)


@pytest.fixture
def subregion_ds():
    return _make_band_dataset([[[1, 1], [1, 1]]], np.int16)


class FakeCarbonQCRepository:
    def load(self, limit=None):
        return gpd.GeoDataFrame({"geometry": [box(0, 0, 1, 1)], "GID_2": ["AFG.1.1_1"]})

    def write_results(self, results, analysis, version):
        pass


class FakeCarbonGEERepository:
    """Returns synthetic GEE data for carbon validation assets.

    emissions per-ha values: [[10, 20], [30, 40]]
    removals per-ha values:  [[5,  10], [15, 20]]
    area per pixel (m²):     [[10000, 10000], [10000, 10000]]

    Expected totals (area_ha = area / 10000 = 1.0 per pixel):
        emissions_Mg_CO2e = 10 + 20 + 30 + 40 = 100.0
        removals_Mg_CO2e  =  5 + 10 + 15 + 20 =  50.0
    """

    def load(self, dataset, geometry, like=None):
        coords = {"y": np.array([0.5, 1.5]), "x": np.array([0.5, 1.5])}
        if dataset == "carbon_gross_emissions":
            return xr.Dataset(
                {
                    "b1": xr.DataArray(
                        np.array([[10.0, 20.0], [30.0, 40.0]], dtype=np.float32),
                        dims=("y", "x"),
                        coords=coords,
                    )
                }
            )
        if dataset == "carbon_gross_removals":
            return xr.Dataset(
                {
                    "b1": xr.DataArray(
                        np.array([[5.0, 10.0], [15.0, 20.0]], dtype=np.float32),
                        dims=("y", "x"),
                        coords=coords,
                    )
                }
            )
        if dataset == "area":
            return xr.Dataset(
                {
                    "area": xr.DataArray(
                        np.array([[10000.0, 10000.0], [10000.0, 10000.0]], dtype=np.float32),
                        dims=("y", "x"),
                        coords=coords,
                    )
                }
            )
        raise ValueError(f"Unknown dataset: {dataset}")
