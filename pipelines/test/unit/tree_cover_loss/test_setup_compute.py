import dask.array as da
import numpy as np
import xarray as xr

from pipelines.tree_cover_loss.stages import TreeCoverLossTasks


def _create_mock_datasets(shape=(2, 2)):
    """Creates mock datasets for TCL unit tests"""
    if shape == (1, 1):
        coords = {"y": [0.0], "x": [0.0]}
        tcl_data = [[1]]
        tcd_data = [[10]]
        ifl_data = [[0]]
        drivers_data = [[1]]
        primary_forests_data = [[0]]
        country_data = [[1]]
        region_data = [[1]]
        subregion_data = [[1]]
        area_data = [[1.0]]
        carbon_data = [[10.0]]
    else:
        coords = {"y": [0.0, 1.0], "x": [0.0, 1.0]}
        tcl_data = [[1, 2], [3, 4]]
        tcd_data = [[10, 20], [30, 40]]
        ifl_data = [[0, 1], [0, 1]]
        drivers_data = [[1, 2], [3, 4]]
        primary_forests_data = [[0, 1], [0, 0]]
        country_data = [[1, 1], [2, 2]]
        region_data = [[1, 1], [2, 2]]
        subregion_data = [[1, 2], [3, 4]]
        area_data = [[1.0, 2.0], [3.0, 4.0]]
        carbon_data = [[10.0, 20.0], [30.0, 40.0]]

    tcl = xr.DataArray(
        da.array(tcl_data, dtype=np.uint8), dims=["y", "x"], coords=coords
    )
    tcd = xr.DataArray(
        da.array(tcd_data, dtype=np.uint8), dims=["y", "x"], coords=coords
    )
    ifl = xr.DataArray(
        da.array(ifl_data, dtype=np.int16), dims=["y", "x"], coords=coords
    )
    drivers = xr.DataArray(
        da.array(drivers_data, dtype=np.int16), dims=["y", "x"], coords=coords
    )
    primary_forests = xr.DataArray(
        da.array(primary_forests_data, dtype=np.uint8), dims=["y", "x"], coords=coords
    )
    country = xr.DataArray(
        da.array(country_data, dtype=np.int16), dims=["y", "x"], coords=coords
    )
    region = xr.DataArray(
        da.array(region_data, dtype=np.uint8), dims=["y", "x"], coords=coords
    )
    subregion = xr.DataArray(
        da.array(subregion_data, dtype=np.int16), dims=["y", "x"], coords=coords
    )

    area_and_emissions = xr.Dataset(
        {
            "area_ha": xr.DataArray(
                da.array(area_data, dtype=np.float32), dims=["y", "x"], coords=coords
            ),
            "carbon__Mg_CO2e": xr.DataArray(
                da.array(carbon_data, dtype=np.float32), dims=["y", "x"], coords=coords
            ),
        }
    )

    return (
        tcl,
        area_and_emissions,
        tcd,
        ifl,
        drivers,
        primary_forests,
        country,
        region,
        subregion,
    )


def test_setup_compute_groupby_schema_and_order():
    """Test that groupby has correct column names, order, and dtypes"""
    datasets = _create_mock_datasets()

    mask, groupbys, _ = TreeCoverLossTasks.setup_compute(datasets, expected_groups=None)

    expected_schema = [
        (0, "tree_cover_loss_year", np.uint8),
        (1, "canopy_cover", np.uint8),
        (2, "is_intact_forest", np.int16),
        (3, "driver", np.int16),
        (4, "is_primary_forest", np.uint8),
        (5, "country", np.int16),
        (6, "region", np.uint8),
        (7, "subregion", np.int16),
    ]

    # validate column order, names, and dtypes
    for column, expected_name, expected_dtype in expected_schema:
        actual_name = groupbys[column].name
        actual_dtype = groupbys[column].dtype

        assert (
            actual_name == expected_name
        ), f"column {column}: expected name '{expected_name}', got '{actual_name}'"
        assert (
            actual_dtype == expected_dtype
        ), f"column {column} ({expected_name}): expected dtype '{expected_dtype}', got '{actual_dtype}'"


def test_setup_compute_creates_concat_dataarray():
    datasets = _create_mock_datasets()
    mask, groupbys, _ = TreeCoverLossTasks.setup_compute(datasets, expected_groups=None)

    # verify layer dimension exists
    assert (
        "layer" in mask.dims
    ), f"mask should have 'layer' dimension, got dims: {mask.dims}"

    # verify layer coord values
    expected_layers = ["area_ha", "carbon_Mg_CO2e"]
    actual_layers = list(mask.coords["layer"].values)
    assert (
        actual_layers == expected_layers
    ), f"expected layers {expected_layers}, got {actual_layers}"

    # verify shape is 2 (for area and emissions)
    assert (
        mask.shape[0] == 2
    ), f"first dimension should be 2 (area & emissions), got {mask.shape[0]}"

    # verify dtype is float
    assert mask.dtype in [
        np.float32,
        np.float64,
    ], f"mask should be float type, got {mask.dtype}"
