import numpy as np

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
        "land_state",
        "land_state_meaning",
        "land_state_broad_class",
        "land_state_detailed_class",
        "tall_veg_type",
        "year",
        "gross_emissions_MgCO2e",
        "gross_removals_MgCO2",
        "net_flux_MgCO2e",
        "area_ha",
    }.issubset(df.columns)

    # rows are keyed by the raw land_state code, not a collapsed class
    assert "land_state_class" not in df.columns
    assert "carbon_pool" not in df.columns
    assert set(df["year"]) == {2016, 2017}
    # all land_states retained, including the no_flux bookkeeping state
    assert 70000000 in set(df["land_state"])

    # subregion-level totals for the tree_loss pixel: emis=20, rem=-8, net=12, area=2
    row = df[
        (df.aoi_id == "BRA.1.1") & (df.land_state == 11100000) & (df.year == 2016)
    ].iloc[0]
    assert row.gross_emissions_MgCO2e == 20.0
    assert row.gross_removals_MgCO2 == -8.0
    assert row.net_flux_MgCO2e == 12.0
    assert row.area_ha == 2.0
    # the lookup attributes are joined onto each land_state
    assert row.land_state_broad_class == "tree"
    assert row.land_state_detailed_class == "tree_loss"
    assert row.tall_veg_type == "mangrove"
    assert row.land_state_meaning == "Gain of mangroves + temp loss in interval"

    # country-level roll-up row exists (single subregion -> same totals)
    country_row = df[
        (df.aoi_id == "BRA") & (df.land_state == 11100000) & (df.year == 2016)
    ].iloc[0]
    assert country_row.gross_emissions_MgCO2e == 20.0

    # net flux is the sum of gross emissions and removals for every row
    assert np.allclose(
        df.net_flux_MgCO2e, df.gross_emissions_MgCO2e + df.gross_removals_MgCO2
    )
