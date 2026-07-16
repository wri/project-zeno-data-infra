import numpy as np

from pipelines.afolu import stages
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
        "component",
        "category",
        "year",
        "gross_emissions_MgCO2e",
        "gross_removals_MgCO2",
        "net_flux_MgCO2e",
        "area_ha",
    }.issubset(df.columns)

    # every vegetation row is tagged with the component; excluded states dropped
    assert set(df["component"]) == {"vegetation"}
    assert "excluded" not in set(df["category"])
    assert set(df["year"]) == {2016, 2017}

    # subregion-level totals for one pixel: emis=20, rem=-8, net=12, area=2
    row = df[
        (df.aoi_id == "BRA.1.1") & (df.category == "tree_loss") & (df.year == 2016)
    ].iloc[0]
    assert row.gross_emissions_MgCO2e == 20.0
    assert row.gross_removals_MgCO2 == -8.0
    assert row.net_flux_MgCO2e == 12.0
    assert row.area_ha == 2.0

    # country-level roll-up row exists (single subregion -> same totals)
    country_row = df[
        (df.aoi_id == "BRA") & (df.category == "tree_loss") & (df.year == 2016)
    ].iloc[0]
    assert country_row.gross_emissions_MgCO2e == 20.0

    # net flux is the sum of gross emissions and removals for every row
    assert np.allclose(
        df.net_flux_MgCO2e, df.gross_emissions_MgCO2e + df.gross_removals_MgCO2
    )
