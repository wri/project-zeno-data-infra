from pipelines.land_ghg_inventory import organic_soil_stages


def test_setup_organic_soil_compute_builds_measure_cube_and_groupbys(
    synthetic_organic_soil_datasets,
):
    datasets, expected_groups = synthetic_organic_soil_datasets

    cube, groupbys, out_expected_groups = (
        organic_soil_stages.setup_organic_soil_compute(datasets, expected_groups)
    )

    assert list(cube.analysis_layer.values) == ["gross_emissions_MgCO2e", "area_ha"]
    # burned (6) + drained (4) = 10 per-hectare, * pixel_area (2 ha) -> 20 per-pixel
    emissions = cube.sel(analysis_layer="gross_emissions_MgCO2e").values
    assert (emissions == 20.0).all()
    area = cube.sel(analysis_layer="area_ha").values
    assert (area == 2.0).all()
    assert list(cube.year.values) == [2020, 2024]

    assert [g.name for g in groupbys] == [
        "country",
        "region",
        "subregion",
        "interval_end_year",
    ]
    assert out_expected_groups is expected_groups
