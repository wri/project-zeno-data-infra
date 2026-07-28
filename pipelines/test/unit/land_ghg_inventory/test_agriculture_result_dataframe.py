from pipelines.land_ghg_inventory import agriculture_stages
from pipelines.prefect_flows import common_stages


def test_agriculture_result_dataframe_rolls_up(synthetic_agriculture_datasets):
    datasets, expected_groups = synthetic_agriculture_datasets

    cube, groupbys, out_expected_groups = agriculture_stages.setup_agriculture_compute(
        datasets, expected_groups
    )
    reduced = common_stages.compute(cube, groupbys, out_expected_groups, "sum")
    df = agriculture_stages.agriculture_result_dataframe(reduced)

    assert {
        "aoi_id",
        "aoi_type",
        "category",
        "gross_emissions_MgCO2e",
    }.issubset(df.columns)
    assert "land_state_class" not in df.columns
    assert "year" not in df.columns
    assert set(df["category"]) == {"cropland", "livestock"}

    # subregion-level totals: top row pixels (10+20, 1+2), one row per
    # category
    subregion_rows = df[df.aoi_id == "BRA.1.1"]
    cropland_row = subregion_rows[subregion_rows.category == "cropland"].iloc[0]
    assert cropland_row.gross_emissions_MgCO2e == 30.0
    livestock_row = subregion_rows[subregion_rows.category == "livestock"].iloc[0]
    assert livestock_row.gross_emissions_MgCO2e == 3.0

    # country-level roll-up: subregion (30, 3) + region-only bottom-left
    # pixel (30, 3)
    country_rows = df[df.aoi_id == "BRA"]
    cropland_country = country_rows[country_rows.category == "cropland"].iloc[0]
    assert cropland_country.gross_emissions_MgCO2e == 60.0
    livestock_country = country_rows[country_rows.category == "livestock"].iloc[0]
    assert livestock_country.gross_emissions_MgCO2e == 6.0
