from pipelines.land_ghg_inventory import mineral_soil_stages
from pipelines.prefect_flows import common_stages


def test_mineral_soil_result_dataframe_rolls_up(synthetic_mineral_soil_datasets):
    datasets, expected_groups = synthetic_mineral_soil_datasets

    cube, groupbys, out_expected_groups = (
        mineral_soil_stages.setup_mineral_soil_compute(datasets, expected_groups)
    )
    reduced = common_stages.compute(cube, groupbys, out_expected_groups, "sum")
    df = mineral_soil_stages.mineral_soil_result_dataframe(reduced)

    assert {
        "aoi_id",
        "aoi_type",
        "gross_emissions_MgCO2e",
        "gross_removals_MgCO2",
        "net_flux_MgCO2e",
        "area_ha",
    }.issubset(df.columns)
    assert "year" not in df.columns
    assert "land_state_class" not in df.columns
    assert "category" not in df.columns
    assert set(df["aoi_type"]) == {"admin"}

    # subregion-level totals: all 4 pixels, one row (no categorical axis)
    subregion_row = df[df.aoi_id == "BRA.1.1"].iloc[0]
    assert subregion_row.gross_emissions_MgCO2e == 80.0
    assert subregion_row.gross_removals_MgCO2 == -32.0
    assert subregion_row.net_flux_MgCO2e == 48.0
    assert subregion_row.area_ha == 8.0

    # country-level roll-up equals the same total (only one admin unit present)
    country_row = df[df.aoi_id == "BRA"].iloc[0]
    assert country_row.gross_emissions_MgCO2e == 80.0
    assert country_row.gross_removals_MgCO2 == -32.0
    assert country_row.net_flux_MgCO2e == 48.0
    assert country_row.area_ha == 8.0
