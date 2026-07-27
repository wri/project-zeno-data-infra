import dask.array as da
import numpy as np
import xarray as xr

from pipelines.land_ghg_inventory import stages
from pipelines.prefect_flows import common_stages


def test_result_dataframe_rolls_up_and_maps(synthetic_datasets):
    datasets, expected_groups = synthetic_datasets

    cube, groupbys, out_expected_groups = stages.setup_vegetation_compute(
        datasets, expected_groups
    )
    reduced = common_stages.compute(cube, groupbys, out_expected_groups, "sum")
    df = stages.vegetation_result_dataframe(reduced)

    assert {
        "aoi_id",
        "aoi_type",
        "land_state_class",
        "year",
        "gross_emissions_MgCO2e",
        "gross_removals_MgCO2",
        "net_flux_MgCO2e",
        "area_ha",
    }.issubset(df.columns)

    # vegetation-only output carries no soil-distinguishing carbon_pool column
    assert "carbon_pool" not in df.columns
    assert "excluded" not in set(df["land_state_class"])
    assert set(df["year"]) == {2016, 2017}

    # subregion-level totals for one pixel: emis=20, rem=-8, net=12, area=2
    row = df[
        (df.aoi_id == "BRA.1.1")
        & (df.land_state_class == "tree_loss")
        & (df.year == 2016)
    ].iloc[0]
    assert row.gross_emissions_MgCO2e == 20.0
    assert row.gross_removals_MgCO2 == -8.0
    assert row.net_flux_MgCO2e == 12.0
    assert row.area_ha == 2.0

    # country-level roll-up row exists (single subregion -> same totals)
    country_row = df[
        (df.aoi_id == "BRA") & (df.land_state_class == "tree_loss") & (df.year == 2016)
    ].iloc[0]
    assert country_row.gross_emissions_MgCO2e == 20.0

    # net flux is the sum of gross emissions and removals for every row
    assert np.allclose(
        df.net_flux_MgCO2e, df.gross_emissions_MgCO2e + df.gross_removals_MgCO2
    )


def test_structurally_zero_measure_is_zero_not_nan():
    # one class (tree_gain) has zero gross emissions, so the sparse reduce stores
    # no cell for it; the output must surface 0.0, not NaN, for downstream apps.
    coords3 = {"year": [0], "y": [0.0, 1.0], "x": [0.0]}

    def band(values):
        arr = np.array(values, dtype="float32").reshape(1, 2, 1)
        return xr.DataArray(
            da.from_array(arr, chunks=(1, 2, 1)),
            dims=["year", "y", "x"],
            coords=coords3,
        )

    veg = xr.Dataset(
        {
            "gross_emissions_MgCO2e": band([10.0, 0.0]),  # tree_loss=10, tree_gain=0
            "gross_removals_MgCO2": band([0.0, -4.0]),
            "net_flux_MgCO2e": band([10.0, -4.0]),
            "land_state_node": xr.DataArray(
                da.from_array(
                    np.array([[[11100000], [21100000]]], dtype="int64"),
                    chunks=(1, 2, 1),
                ),
                dims=["year", "y", "x"],
                coords=coords3,
            ),
        }
    )
    coords2 = {"y": [0.0, 1.0], "x": [0.0]}

    def layer(values, dtype):
        return xr.DataArray(
            da.from_array(np.array(values, dtype=dtype).reshape(2, 1), chunks=(2, 1)),
            dims=["y", "x"],
            coords=coords2,
        )

    datasets = (
        veg,
        layer([2.0, 2.0], "float64"),
        layer([76, 76], "int32"),
        layer([1, 1], "int32"),
        layer([1, 1], "int32"),
    )
    expected_groups = (
        np.array([76]),
        np.array([1]),
        np.array([1]),
        np.array([0, 1, 2, 3, 4]),
        np.array([0]),
    )
    cube, groupbys, out_expected_groups = stages.setup_vegetation_compute(
        datasets, expected_groups
    )
    reduced = common_stages.compute(cube, groupbys, out_expected_groups, "sum")
    df = stages.vegetation_result_dataframe(reduced)

    measures = ["gross_emissions_MgCO2e", "gross_removals_MgCO2", "net_flux_MgCO2e"]
    assert df[measures + ["area_ha"]].notna().all().all()
    tree_gain = df[(df.aoi_id == "BRA") & (df.land_state_class == "tree_gain")].iloc[0]
    assert tree_gain.gross_emissions_MgCO2e == 0.0
