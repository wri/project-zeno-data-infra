from pipelines.land_ghg_inventory import mineral_soil_stages


def test_setup_mineral_soil_compute_builds_measure_cube_and_groupbys(
    synthetic_mineral_soil_datasets,
):
    datasets, expected_groups = synthetic_mineral_soil_datasets

    cube, groupbys, out_expected_groups = (
        mineral_soil_stages.setup_mineral_soil_compute(datasets, expected_groups)
    )

    assert list(cube.analysis_layer.values) == [
        "gross_emissions_MgCO2e",
        "gross_removals_MgCO2",
        "net_flux_MgCO2e",
        "area_ha",
    ]
    # per-hectare * pixel_area (2 ha) -> per-pixel totals
    emissions = cube.sel(analysis_layer="gross_emissions_MgCO2e").values
    assert (emissions == 20.0).all()
    removals = cube.sel(analysis_layer="gross_removals_MgCO2").values
    assert (removals == -8.0).all()
    net = cube.sel(analysis_layer="net_flux_MgCO2e").values
    assert (net == 12.0).all()
    area = cube.sel(analysis_layer="area_ha").values
    assert (area == 2.0).all()

    assert [g.name for g in groupbys] == ["country", "region", "subregion"]
    assert out_expected_groups is expected_groups
