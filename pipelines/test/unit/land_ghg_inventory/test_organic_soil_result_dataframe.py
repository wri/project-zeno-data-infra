from pipelines.land_ghg_inventory import organic_soil_stages
from pipelines.prefect_flows import common_stages


def test_organic_soil_result_dataframe_rolls_up(synthetic_organic_soil_datasets):
    datasets, expected_groups = synthetic_organic_soil_datasets

    cube, groupbys, out_expected_groups = (
        organic_soil_stages.setup_organic_soil_compute(datasets, expected_groups)
    )
    reduced = common_stages.compute(cube, groupbys, out_expected_groups, "sum")
    df = organic_soil_stages.organic_soil_result_dataframe(reduced)

    assert {
        "aoi_id",
        "aoi_type",
        "interval_end_year",
        "gross_emissions_MgCO2e",
        "area_ha",
    }.issubset(df.columns)
    assert "year" not in df.columns
    assert "land_state_class" not in df.columns
    assert "category" not in df.columns
    assert set(df["aoi_type"]) == {"admin"}
    # native block labels persisted as-is, not expanded to vegetation years
    assert set(df["interval_end_year"]) == {2020, 2024}

    # subregion-level totals: all 4 pixels, one row per block year
    subregion_rows = df[df.aoi_id == "BRA.1.1"]
    for year in (2020, 2024):
        row = subregion_rows[subregion_rows.interval_end_year == year].iloc[0]
        assert row.gross_emissions_MgCO2e == 80.0
        assert row.area_ha == 8.0

    # country-level roll-up equals the same total (only one admin unit present)
    country_rows = df[df.aoi_id == "BRA"]
    for year in (2020, 2024):
        row = country_rows[country_rows.interval_end_year == year].iloc[0]
        assert row.gross_emissions_MgCO2e == 80.0
        assert row.area_ha == 8.0
