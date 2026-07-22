import dask.array as da
import numpy as np
import pytest
import xarray as xr


@pytest.fixture
def synthetic_datasets():
    """Tiny 2-year, 2x2 scene with one pixel per vegetation category.

    Per-hectare fluxes are constant (emis=10, rem=-4, net=6) and each 2 ha pixel
    maps to a distinct land_state category, so grouped per-pixel totals are known:
    emis=20, rem=-8, net=12, area=2.
    """
    coords3 = {"year": [0, 1], "y": [0.0, 1.0], "x": [0.0, 1.0]}

    def cube(value):
        arr = np.full((2, 2, 2), value, dtype="float32")
        return xr.DataArray(
            da.from_array(arr, chunks=(1, 2, 2)),
            dims=["year", "y", "x"],
            coords=coords3,
        )

    land_state_2d = np.array(
        [[11100000, 21100000], [13200000, 70000000]], dtype="int64"
    )  # tree_loss, tree_gain / tree_tree_undisturbed, no_flux
    land_state = np.stack([land_state_2d, land_state_2d])

    veg = xr.Dataset(
        {
            "gross_emissions_MgCO2e": cube(10.0),
            "gross_removals_MgCO2": cube(-4.0),
            "net_flux_MgCO2e": cube(6.0),
            "land_state_node": xr.DataArray(
                da.from_array(land_state, chunks=(1, 2, 2)),
                dims=["year", "y", "x"],
                coords=coords3,
            ),
        }
    )

    coords2 = {"y": [0.0, 1.0], "x": [0.0, 1.0]}

    def layer(values, dtype):
        return xr.DataArray(
            da.from_array(np.array(values, dtype=dtype), chunks=(2, 2)),
            dims=["y", "x"],
            coords=coords2,
        )

    pixel_area = layer([[2.0, 2.0], [2.0, 2.0]], "float64")
    country = layer([[76, 76], [76, 76]], "int32")  # 76 -> BRA
    region = layer([[1, 1], [1, 1]], "int32")
    subregion = layer([[1, 1], [1, 1]], "int32")

    datasets = (veg, pixel_area, country, region, subregion)
    expected_groups = (
        np.array([76]),
        np.array([1]),
        np.array([1]),
        # raw land_state codes present in the scene (no longer collapsed to 0-4)
        np.array([11100000, 13200000, 21100000, 70000000]),
        np.array([0, 1]),
    )
    return datasets, expected_groups
