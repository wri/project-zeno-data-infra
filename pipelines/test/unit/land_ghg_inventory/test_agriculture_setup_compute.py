from pipelines.land_ghg_inventory import agriculture_stages


def test_setup_agriculture_compute_builds_measure_cube_and_groupbys(
    synthetic_agriculture_datasets,
):
    datasets, expected_groups = synthetic_agriculture_datasets

    cube, groupbys, out_expected_groups = agriculture_stages.setup_agriculture_compute(
        datasets, expected_groups
    )

    assert list(cube.analysis_layer.values) == [
        "cropland_emissions",
        "livestock_emissions",
    ]
    # values pass through unchanged (already per-pixel absolute totals)
    cropland = cube.sel(analysis_layer="cropland_emissions").values
    assert (cropland == [[10.0, 20.0], [30.0, 0.0]]).all()
    livestock = cube.sel(analysis_layer="livestock_emissions").values
    assert (livestock == [[1.0, 2.0], [3.0, 0.0]]).all()

    assert [g.name for g in groupbys] == ["country", "region", "subregion"]
    assert out_expected_groups is expected_groups
