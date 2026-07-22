"""Integration test: the vegetation stages run against the real global zarrs,
clipped to São Tomé & Príncipe, must reproduce known reference totals. Exercises
the same chain the flow runs (load -> setup -> reduce -> result dataframe), minus
the global scope and the S3 write.
"""

import numpy as np
import pytest
from shapely.geometry import box

from pipelines.globals import (
    country_zarr_uri,
    land_ghg_inventory_vegetation_zarr_uri,
    pixel_area_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.land_ghg_inventory import stages
from pipelines.land_ghg_inventory.land_state_categories import LAND_STATE_CODES
from pipelines.prefect_flows import common_stages

# São Tomé & Príncipe: both islands, ocean elsewhere (isolated -> clean bbox).
STP_BBOX = box(6.4, -0.05, 7.5, 1.8)

# Known reference totals for STP, summed over 2016-2024 (MgCO2e), validated
# externally. Hard-coded so the test never reads the reference dataset itself.
EXPECTED_TOTALS = {
    "gross_emissions_MgCO2e": 10426.5,
    "net_flux_MgCO2e": -11655664.0,
}


def test_stp_reproduces_reference_totals():
    datasets = stages.load_data(
        land_ghg_inventory_vegetation_zarr_uri,
        pixel_area_zarr_uri,
        country_zarr_uri,
        region_zarr_uri,
        subregion_zarr_uri,
        bbox=STP_BBOX,
    )
    expected_groups = (
        np.arange(999),
        np.arange(86),
        np.arange(854),
        np.array(LAND_STATE_CODES),
        np.arange(9),
    )
    cube, groupbys, out_expected_groups = stages.setup_vegetation_compute(
        datasets, expected_groups
    )
    reduced = common_stages.compute(cube, groupbys, out_expected_groups, "sum")
    df = stages.vegetation_result_dataframe(reduced)

    country = df[(df["aoi_id"] == "STP")]
    assert not country.empty
    totals = country[["gross_emissions_MgCO2e", "net_flux_MgCO2e"]].sum()
    assert totals["gross_emissions_MgCO2e"] == pytest.approx(
        EXPECTED_TOTALS["gross_emissions_MgCO2e"], rel=0.02
    )
    assert totals["net_flux_MgCO2e"] == pytest.approx(
        EXPECTED_TOTALS["net_flux_MgCO2e"], rel=0.02
    )

    # sign sanity: tree_gain is removals-only (net sink), tree_loss is a source
    by_detailed = country.groupby("land_state_detailed_class")[
        ["gross_emissions_MgCO2e", "net_flux_MgCO2e"]
    ].sum()
    assert by_detailed.loc["tree_gain", "gross_emissions_MgCO2e"] == pytest.approx(0.0)
    assert by_detailed.loc["tree_gain", "net_flux_MgCO2e"] < 0
    assert by_detailed.loc["tree_loss", "net_flux_MgCO2e"] > 0
