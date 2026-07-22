import numpy as np

from pipelines.land_ghg_inventory import stages


def test_setup_compute_builds_flux_cube_and_groupbys(synthetic_datasets):
    datasets, expected_groups = synthetic_datasets

    cube, groupbys, out_expected_groups = stages.setup_vegetation_compute(
        datasets, expected_groups
    )

    assert list(cube.analysis_layer.values) == [
        "gross_emissions_MgCO2e",
        "gross_removals_MgCO2",
        "net_flux_MgCO2e",
        "area_ha",
    ]
    # per-hectare flux x pixel area (10/ha * 2 ha = 20)
    emissions = cube.sel(analysis_layer="gross_emissions_MgCO2e").isel(year=0).values
    assert (emissions == 20.0).all()
    # area layer carries the pixel area itself
    area = cube.sel(analysis_layer="area_ha").isel(year=0).values
    assert (area == 2.0).all()
    # net = emissions + removals at the pixel level (6/ha * 2 = 12)
    net = cube.sel(analysis_layer="net_flux_MgCO2e").isel(year=0).values
    assert (net == 12.0).all()

    assert [g.name for g in groupbys] == [
        "country",
        "region",
        "subregion",
        "land_state",
        "year",
    ]
    # grouped by the raw land_state codes, not the collapsed 0-4 categories
    land_state = next(g for g in groupbys if g.name == "land_state")
    assert set(np.unique(land_state.values)) == {
        11100000,
        13200000,
        21100000,
        70000000,
    }
    assert out_expected_groups is expected_groups
