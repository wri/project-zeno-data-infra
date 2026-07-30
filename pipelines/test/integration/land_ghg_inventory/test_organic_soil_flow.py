"""Integration test: the organic_soil stages run against the real global zarr,
clipped to São Tomé & Príncipe, must reproduce known reference totals. Exercises
the same chain the flow runs (load -> setup -> reduce -> result dataframe), minus
the global scope and the S3 write.

Only emissions are asserted here, not area_ha: STP has zero organic_soil-extent
pixels in the current zarr, so after the area-mask fix its masked area is 0 ha,
not the country's total land area -- emissions are unaffected by that mask (they
were already zero outside the extent) and stay a stable, meaningful check.
"""

import numpy as np
from shapely.geometry import box

from pipelines.globals import (
    country_zarr_uri,
    gadm_country_code_count,
    gadm_region_code_count,
    gadm_subregion_code_count,
    land_ghg_inventory_organic_soil_zarr_uri,
    pixel_area_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.land_ghg_inventory import organic_soil_stages
from pipelines.prefect_flows import common_stages

# São Tomé & Príncipe: both islands, ocean elsewhere (isolated -> clean bbox).
STP_BBOX = box(6.4, -0.05, 7.5, 1.8)


def test_stp_reproduces_reference_totals():
    datasets = organic_soil_stages.load_data(
        land_ghg_inventory_organic_soil_zarr_uri,
        pixel_area_zarr_uri,
        country_zarr_uri,
        region_zarr_uri,
        subregion_zarr_uri,
        bbox=STP_BBOX,
    )
    expected_groups = (
        np.arange(gadm_country_code_count),
        np.arange(gadm_region_code_count),
        np.arange(gadm_subregion_code_count),
        np.array([2020, 2024]),
    )
    cube, groupbys, out_expected_groups = (
        organic_soil_stages.setup_organic_soil_compute(datasets, expected_groups)
    )
    reduced = common_stages.compute(cube, groupbys, out_expected_groups, "sum")
    df = organic_soil_stages.organic_soil_result_dataframe(reduced)

    country = df[df["aoi_id"] == "STP"]
    assert not country.empty
    assert set(country["interval_end_year"]) == {2020, 2024}
    for year in (2020, 2024):
        row = country[country["interval_end_year"] == year].iloc[0]
        assert row["gross_emissions_MgCO2e"] == 0.0
