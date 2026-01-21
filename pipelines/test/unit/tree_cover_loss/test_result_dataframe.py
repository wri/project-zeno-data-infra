import numpy as np
import pandas as pd
import xarray as xr
import sparse

from pipelines.tree_cover_loss.stages import create_result_dataframe_multi_var


def _create_sparse_dataset(area_coords, area_data, carbon_coords, carbon_data, dims, coords, shape):
    """Helper to create xr. Dataset with sparse arrays"""
    area_sparse = sparse.COO(area_coords, area_data, shape=shape)
    carbon_sparse = sparse.COO(carbon_coords, carbon_data, shape=shape)

    return xr.Dataset(
        {
            "area_ha": xr.DataArray(area_sparse, dims=dims),
            "carbon__Mg_CO2e": xr.DataArray(carbon_sparse, dims=dims),
        },
        coords=coords,
    )


def test_create_result_dataframe_same_sparsity_pattern():
    """Test df when pixel area and carbon have identical sparsity patterns"""
    # create sparse arrays with same non-zero locations at (0,0), (1,1), (2,0)
    area_coords = np.array([[0, 1, 2], [0, 1, 0]])
    area_data = np.array([10.0, 20.0, 30.0])
    carbon_coords = np.array([[0, 1, 2], [0, 1, 0]])
    carbon_data = np.array([100.0, 200.0, 300.0])

    result_dataset = _create_sparse_dataset(
        area_coords, area_data, carbon_coords, carbon_data,
        dims=["year", "country"],
        coords={"year": np.array([2001, 2002, 2003]), "country": np.array([1, 2])},
        shape=(3, 2)
    )

    df = create_result_dataframe_multi_var(result_dataset)

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


def test_create_result_dataframe_different_sparsity_patterns():
    """Test df when carbon is present but not pixel area"""
    # pixel area has data at (0,0) and (1,1)
    area_coords = np.array([[0, 1], [0, 1]])
    area_data = np.array([10.0, 20.0])

    # carbon has data at (0,0), (1,1), and (2,0)
    carbon_coords = np.array([[0, 1, 2], [0, 1, 0]])
    carbon_data = np.array([100.0, 200.0, 300.0])

    result_dataset = _create_sparse_dataset(
        area_coords, area_data, carbon_coords, carbon_data,
        dims=["year", "country"],
        coords={"year": np.array([2001, 2002, 2003]), "country": np.array([1, 2])},
        shape=(3, 2)
    )

    df = create_result_dataframe_multi_var(result_dataset)

    # df should have rows only where pixel area exists
    assert len(df) == 2, f"expected 2 rows (area locations only), got {len(df)}"
    assert not any((df["year"] == 2003) & (df["country"] == 1))

    # verify carbon values are aligned with pixel area
    row_0_0 = df[(df["year"] == 2001) & (df["country"] == 1)]
    assert len(row_0_0) == 1
    assert row_0_0["area_ha"].iloc[0] == 10.0
    assert row_0_0["carbon_Mg_CO2e"].iloc[0] == 100.0


def test_create_result_dataframe_missing_carbon_fills_zero():
    """Test that missing carbon values are filled with 0.0 where pixel area exists"""
    # area has data at (0,0) and (1,1)
    area_coords = np.array([[0, 1], [0, 1]])
    area_data = np.array([10.0, 20.0])

    # carbon only has data at (0,0) only
    carbon_coords = np.array([[0], [0]])
    carbon_data = np.array([100.0])

    result_dataset = _create_sparse_dataset(
        area_coords, area_data, carbon_coords, carbon_data,
        dims=["year", "country"],
        coords={"year": np.array([2001, 2002, 2003]), "country": np.array([1, 2])},
        shape=(3, 2)
    )

    df = create_result_dataframe_multi_var(result_dataset)

    assert len(df) == 2, f"expected 2 rows, got {len(df)}"

    # verify (0,0) has both pixel area and carbon
    row_0_0 = df[(df["year"] == 2001) & (df["country"] == 1)]
    assert len(row_0_0) == 1
    assert row_0_0["area_ha"].iloc[0] == 10.0
    assert row_0_0["carbon_Mg_CO2e"].iloc[0] == 100.0

    # verify (1,1) has pixel area only
    row_1_1 = df[(df["year"] == 2002) & (df["country"] == 2)]
    assert len(row_1_1) == 1
    assert row_1_1["area_ha"].iloc[0] == 20.0
    assert row_1_1["carbon_Mg_CO2e"].iloc[0] == 0.0, "missing carbon should be filled with 0.0"


def test_create_result_dataframe_output_schema():
    """Test that output df has correct column names and dtypes"""
    area_coords = np.array([[0], [0]])
    area_data = np.array([10.5])
    carbon_coords = np.array([[0], [0]])
    carbon_data = np.array([100.5])

    result_dataset = _create_sparse_dataset(
        area_coords, area_data, carbon_coords, carbon_data,
        dims=["tree_cover_loss_year", "country"],
        coords={"tree_cover_loss_year": np.array([1, 2]), "country": np.array([100, 200])},
        shape=(2, 2)
    )

    df = create_result_dataframe_multi_var(result_dataset)

    # verify column names and dtypes
    expected_columns = {"tree_cover_loss_year", "country", "area_ha", "carbon_Mg_CO2e"}
    assert set(df.columns) == expected_columns, f"expected columns {expected_columns}, got {set(df.columns)}"
    assert df["area_ha"].dtype in [np.float32, np.float64], f"area should be float, got {df['area_ha'].dtype}"
    assert df["carbon_Mg_CO2e"].dtype in [np.float32, np.float64], f"carbon should be float, got {df['carbon_Mg_CO2e'].dtype}"
