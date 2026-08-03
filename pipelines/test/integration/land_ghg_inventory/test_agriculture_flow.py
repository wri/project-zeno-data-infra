"""Integration test: the agriculture stages run against the real global zarr,
clipped to São Tomé & Príncipe, must reproduce known reference totals. Exercises
the same chain the flow runs (load -> setup -> reduce -> result dataframe), minus
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
    land_ghg_inventory_agriculture_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.land_ghg_inventory import agriculture_stages
from pipelines.prefect_flows import common_stages

# São Tomé & Príncipe: both islands, ocean elsewhere (isolated -> clean bbox).
STP_BBOX = box(6.4, -0.05, 7.5, 1.8)

# Reference totals for STP, computed directly against the real zarr (no reduce over
# the full gadm_*_code_count admin codes changes these, since STP's own codes are
# within range).
EXPECTED_TOTALS = {
    "cropland": 1.7105374e04,
    "livestock": 1.2441838e04,
}


def test_stp_reproduces_reference_totals():
    datasets = agriculture_stages.load_agriculture(
        land_ghg_inventory_agriculture_zarr_uri,
        country_zarr_uri,
        region_zarr_uri,
        subregion_zarr_uri,
        bbox=STP_BBOX,
    )
    expected_groups = (
        np.arange(gadm_country_code_count),
        np.arange(gadm_region_code_count),
        np.arange(gadm_subregion_code_count),
    )
    cube, groupbys, out_expected_groups = agriculture_stages.setup_agriculture_compute(
        datasets, expected_groups
    )
    reduced = common_stages.compute(cube, groupbys, out_expected_groups, "sum")
    df = agriculture_stages.agriculture_result_dataframe(reduced)

    country = df[df["aoi_id"] == "STP"]
    assert not country.empty
    totals = country.groupby("category")["gross_emissions_MgCO2e"].sum()
    assert totals["cropland"] == pytest.approx(EXPECTED_TOTALS["cropland"], rel=0.02)
    assert totals["livestock"] == pytest.approx(EXPECTED_TOTALS["livestock"], rel=0.02)
