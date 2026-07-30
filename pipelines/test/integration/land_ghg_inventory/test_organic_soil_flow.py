"""Integration test: the organic_soil stages run against the real global zarr,
clipped to Singapore, must reproduce known reference emissions. Exercises the
same chain the flow runs (load -> setup -> reduce -> result dataframe), minus
the global scope and the S3 write.
"""

import numpy as np
import pytest
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

# Singapore: compact city-state, isolated by water -> clean bbox.
SGP_BBOX = box(103.6, 1.15, 104.1, 1.48)

# Reference emissions for SGP, computed directly against the real zarr (no
# reduce over the full gadm_*_code_count admin codes changes these, since SGP's
# own codes are within range).
EXPECTED_EMISSIONS_MgCO2e = {
    2020: 45987.43,
    2024: 67217.00,
}


def test_sgp_reproduces_reference_totals():
    datasets = organic_soil_stages.load_data(
        land_ghg_inventory_organic_soil_zarr_uri,
        pixel_area_zarr_uri,
        country_zarr_uri,
        region_zarr_uri,
        subregion_zarr_uri,
        bbox=SGP_BBOX,
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

    country = df[df["aoi_id"] == "SGP"]
    assert not country.empty
    assert set(country["interval_end_year"]) == {2020, 2024}
    for year in (2020, 2024):
        row = country[country["interval_end_year"] == year].iloc[0]
        assert row["gross_emissions_MgCO2e"] == pytest.approx(
            EXPECTED_EMISSIONS_MgCO2e[year], rel=0.02
        )
