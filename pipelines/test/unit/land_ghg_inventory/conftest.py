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
    )  # -> categories [[tree_loss, tree_gain], [trees_remaining, excluded]]
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
        np.array([0, 1, 2, 3, 4]),
        np.array([0, 1]),
    )
    return datasets, expected_groups


@pytest.fixture
def synthetic_agriculture_datasets():
    """Tiny 2x2 scene, two admin units (BRA subregion 1 and BRA country-only).

    Agriculture values are already per-pixel absolute totals (no pixel-area
    multiplication, no year/land_state axis). Per-pixel totals are known:
    cropland=[[10, 20], [30, 0]], livestock=[[5, 0], [15, 0]].
    """
    coords2 = {"y": [0.0, 1.0], "x": [0.0, 1.0]}

    def layer(values, dtype="float32"):
        return xr.DataArray(
            da.from_array(np.array(values, dtype=dtype), chunks=(2, 2)),
            dims=["y", "x"],
            coords=coords2,
        )

    ag = xr.Dataset(
        {
            "cropland": layer([[10.0, 20.0], [30.0, 0.0]]),
            "livestock": layer([[5.0, 0.0], [15.0, 0.0]]),
        }
    )

    # top row -> BRA subregion 1; bottom-left -> BRA subregion 0 (region-only);
    # bottom-right -> no country (dropped, like ocean pixels).
    country = layer([[76, 76], [76, 0]], "int32")
    region = layer([[1, 1], [1, 0]], "int32")
    subregion = layer([[1, 1], [0, 0]], "int32")

    datasets = (ag, country, region, subregion)
    expected_groups = (
        np.array([76]),
        np.array([1]),
        np.array([0, 1]),
    )
    return datasets, expected_groups


@pytest.fixture
def synthetic_mineral_soil_datasets():
    """Tiny 2x2 scene, single time slice already selected (no year axis).

    Per-hectare SOC fluxes are constant (emis=10, rem=-4, net=6) and each 2 ha
    pixel maps to the same admin unit, so grouped per-pixel totals are known:
    emis=20, rem=-8, net=12, area=2.
    """
    coords2 = {"y": [0.0, 1.0], "x": [0.0, 1.0]}

    def layer(values, dtype="float32"):
        return xr.DataArray(
            da.from_array(np.array(values, dtype=dtype), chunks=(2, 2)),
            dims=["y", "x"],
            coords=coords2,
        )

    soc = xr.Dataset(
        {
            "gross_emissions_MgCO2e": layer([[10.0, 10.0], [10.0, 10.0]]),
            "gross_removals_MgCO2": layer([[-4.0, -4.0], [-4.0, -4.0]]),
            "net_flux_MgCO2e": layer([[6.0, 6.0], [6.0, 6.0]]),
        }
    )
    pixel_area = layer([[2.0, 2.0], [2.0, 2.0]], "float64")
    country = layer([[76, 76], [76, 76]], "int32")  # 76 -> BRA
    region = layer([[1, 1], [1, 1]], "int32")
    subregion = layer([[1, 1], [1, 1]], "int32")

    datasets = (soc, pixel_area, country, region, subregion)
    expected_groups = (
        np.array([76]),
        np.array([1]),
        np.array([1]),
    )
    return datasets, expected_groups


@pytest.fixture
def synthetic_organic_soil_datasets():
    """Tiny 2-block, 2x2 scene with a year dim of exactly 2 values [2020, 2024]

    Per-hectare burned/drained fluxes are constant (burned=6, drained=4, so
    emissions=10) on 3 of the 4 pixels; the 4th pixel is outside the
    organic_soil extent mask and has zero burned/drained (matching the real
    zarr's invariant that fluxes are zero outside the mask). Each 2 ha pixel
    maps to the same admin unit, so grouped per-pixel totals are known:
    emissions=60 (3 pixels x 20), area=6 ha (3 pixels x 2 ha, mask excludes
    the 4th), for each block year.
    """
    coords3 = {"year": [2020, 2024], "y": [0.0, 1.0], "x": [0.0, 1.0]}

    def cube3(values):
        arr = np.array([values, values], dtype="float32")
        return xr.DataArray(
            da.from_array(arr, chunks=(1, 2, 2)),
            dims=["year", "y", "x"],
            coords=coords3,
        )

    org = xr.Dataset(
        {
            "burned_total_Mg_CO2e_ha_yr": cube3([[6.0, 6.0], [6.0, 0.0]]),
            "drained_total_Mg_CO2e_ha_yr": cube3([[4.0, 4.0], [4.0, 0.0]]),
            "organic_soil": cube3([[1, 1], [1, 0]]).astype("uint8"),
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

    datasets = (org, pixel_area, country, region, subregion)
    expected_groups = (
        np.array([76]),
        np.array([1]),
        np.array([1]),
        np.array([2020, 2024]),
    )
    return datasets, expected_groups
