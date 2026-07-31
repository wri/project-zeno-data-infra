"""Integration test: the mineral_soil stages run against the real global zarr,
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
    land_ghg_inventory_soc_zarr_uri,
    pixel_area_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.land_ghg_inventory import mineral_soil_stages
from pipelines.prefect_flows import common_stages

# São Tomé & Príncipe: both islands, ocean elsewhere (isolated -> clean bbox).
STP_BBOX = box(6.4, -0.05, 7.5, 1.8)

# Reference totals for STP, computed directly against the real zarr (no reduce over
# the full gadm_*_code_count admin codes changes these, since STP's own codes are
# within range). Emissions/removals/net flux are genuinely zero for STP over the
# 2015-2020 change interval -- a small island nation with minimal recorded mineral
# SOC change in this dataset, not a computation bug (verified against a nonzero
# Amazon bbox during development).
EXPECTED_AREA_HA = 100217.98772067629


def test_stp_reproduces_reference_totals():
    datasets = mineral_soil_stages.load_data(
        land_ghg_inventory_soc_zarr_uri,
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
    )
    cube, groupbys, out_expected_groups = (
        mineral_soil_stages.setup_mineral_soil_compute(datasets, expected_groups)
    )
    reduced = common_stages.compute(cube, groupbys, out_expected_groups, "sum")
    df = mineral_soil_stages.mineral_soil_result_dataframe(reduced)

    country = df[df["aoi_id"] == "STP"]
    assert not country.empty
    row = country.iloc[0]
    assert row["gross_emissions_MgCO2e"] == 0.0
    assert row["gross_removals_MgCO2"] == 0.0
    assert row["net_flux_MgCO2e"] == 0.0
    assert row["area_ha"] == pytest.approx(EXPECTED_AREA_HA, rel=0.02)
