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
        "cropland_emissions",
        "livestock_emissions",
    }.issubset(df.columns)
    assert "land_state_class" not in df.columns
    assert "year" not in df.columns

    # subregion-level totals: top row pixels (10+20, 1+2)
    subregion_row = df[df.aoi_id == "BRA.1.1"].iloc[0]
    assert subregion_row.cropland_emissions == 30.0
    assert subregion_row.livestock_emissions == 3.0

    # country-level roll-up: subregion (30, 3) + region-only bottom-left pixel (30, 3)
    country_row = df[df.aoi_id == "BRA"].iloc[0]
    assert country_row.cropland_emissions == 60.0
    assert country_row.livestock_emissions == 6.0
