import numpy as np
import pandas as pd
import xarray as xr
import sparse

from pipelines.tree_cover_loss.stages import create_result_dataframe


def _create_sparse_dataarray(area_coords, area_data, carbon_coords, carbon_data, dims, coords, shape):
    """Helper to create xr.DataArray with layer dimension using xr.concat pattern"""
    area_sparse = sparse.COO(area_coords, area_data, shape=shape)
    carbon_sparse = sparse.COO(carbon_coords, carbon_data, shape=shape)

    area_da = xr.DataArray(area_sparse, dims=dims, coords=coords)
    carbon_da = xr.DataArray(carbon_sparse, dims=dims, coords=coords)

    # concatenate along layer dimension (following carbon flux pattern)
    return xr.concat(
        [area_da, carbon_da],
        pd.Index(["area_ha", "carbon_Mg_CO2e"], name="layer")
    )


def test_create_result_dataframe_same_sparsity_pattern():
    """Test df when pixel area and carbon have identical sparsity patterns"""
    # create sparse arrays with same non-zero locations at (0,0), (1,1), (2,0)
    area_coords = np.array([[0, 1, 2], [0, 1, 0]])
    area_data = np.array([10.0, 20.0, 30.0])
    carbon_coords = np.array([[0, 1, 2], [0, 1, 0]])
    carbon_data = np.array([100.0, 200.0, 300.0])

    result_dataarray = _create_sparse_dataarray(
        area_coords, area_data, carbon_coords, carbon_data,
        dims=["year", "country"],
        coords={"year": np.array([2001, 2002, 2003]), "country": np.array([1, 2])},
        shape=(3, 2)
    )

    df = create_result_dataframe(result_dataarray)

    # verify correct shape and columns
    assert len(df) == 3, f"expected 3 rows, got {len(df)}"
    assert "year" in df.columns
    assert "country" in df.columns
    assert "area_ha" in df.columns
    assert "carbon_Mg_CO2e" in df.columns

    # verify values
    assert df["area_ha"].sum() == 60.0, f"expected area sum 60.0, got {df['area_ha'].sum()}"
    assert df["carbon_Mg_CO2e"].sum() == 600.0, f"expected carbon sum 600.0, got {df['carbon_Mg_CO2e'].sum()}"

    # verify specific rows
    row_0_0 = df[(df["year"] == 2001) & (df["country"] == 1)]
    assert len(row_0_0) == 1
    assert row_0_0["area_ha"].iloc[0] == 10.0
    assert row_0_0["carbon_Mg_CO2e"].iloc[0] == 100.0


def test_create_result_dataframe_mismatched_sparsity():
    """Test handling of mismatched sparsity patterns"""
    # area at (0,0) & (1,1), carbon at (0,0) & (2,0)
    area_coords = np.array([[0, 1], [0, 1]])
    area_data = np.array([10.0, 20.0])
    carbon_coords = np.array([[0, 2], [0, 0]])
    carbon_data = np.array([100.0, 300.0])

    result_dataarray = _create_sparse_dataarray(
        area_coords, area_data, carbon_coords, carbon_data,
        dims=["year", "country"],
        coords={"year": np.array([2001, 2002, 2003]), "country": np.array([1, 2])},
        shape=(3, 2)
    )

    df = create_result_dataframe(result_dataarray)

    assert len(df) == 3, f"expected 3 rows (union of all coordinates), got {len(df)}"

    # row (0,0): both area and carbon
    row_0_0 = df[(df["year"] == 2001) & (df["country"] == 1)]
    assert len(row_0_0) == 1
    assert row_0_0["area_ha"].iloc[0] == 10.0
    assert row_0_0["carbon_Mg_CO2e"].iloc[0] == 100.0

    # row (1,1): area but no carbon
    row_1_1 = df[(df["year"] == 2002) & (df["country"] == 2)]
    assert len(row_1_1) == 1
    assert row_1_1["area_ha"].iloc[0] == 20.0
    assert row_1_1["carbon_Mg_CO2e"].iloc[0] == 0.0, "missing carbon should be filled with 0.0"

    # row (2,0): carbon but no area
    row_2_0 = df[(df["year"] == 2003) & (df["country"] == 1)]
    assert len(row_2_0) == 1
    assert row_2_0["area_ha"].iloc[0] == 0.0, "missing area should be filled with 0.0"
    assert row_2_0["carbon_Mg_CO2e"].iloc[0] == 300.0


def test_create_result_dataframe_output_schema():
    """Test that output df has correct column names and dtypes"""
    area_coords = np.array([[0], [0]])
    area_data = np.array([10.5])
    carbon_coords = np.array([[0], [0]])
    carbon_data = np.array([100.5])

    result_dataarray = _create_sparse_dataarray(
        area_coords, area_data, carbon_coords, carbon_data,
        dims=["tree_cover_loss_year", "country"],
        coords={"tree_cover_loss_year": np.array([1, 2]), "country": np.array([100, 200])},
        shape=(2, 2)
    )

    df = create_result_dataframe(result_dataarray)

    # verify column names and dtypes
    expected_columns = {"tree_cover_loss_year", "country", "area_ha", "carbon_Mg_CO2e"}
    assert set(df.columns) == expected_columns, f"expected columns {expected_columns}, got {set(df.columns)}"
    assert df["area_ha"].dtype in [np.float32, np.float64], f"area should be float, got {df['area_ha'].dtype}"
    assert df["carbon_Mg_CO2e"].dtype in [np.float32, np.float64], f"carbon should be float, got {df['carbon_Mg_CO2e'].dtype}"
