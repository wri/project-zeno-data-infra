"""Integration test: the vegetation stages run against the real global zarrs,
clipped to São Tomé & Príncipe, must reproduce known reference totals. Exercises
the same chain the flow runs (load -> setup -> reduce -> result dataframe), minus
the global scope and the S3 write.
"""

import numpy as np
import pytest
from shapely.geometry import box

from pipelines.afolu import stages
from pipelines.globals import (
    afolu_soc_zarr_uri,
    afolu_vegetation_zarr_uri,
    country_zarr_uri,
    pixel_area_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.prefect_flows import common_stages

# São Tomé & Príncipe: both islands, ocean elsewhere (isolated -> clean bbox).
STP_BBOX = box(6.4, -0.05, 7.5, 1.8)

# Brunei: compact, mineral-soil signal; reference mineral SOC net is a constant
# annual rate (period-3 broadcast) of ~107,271.9 MgCO2e/yr.
BRUNEI_BBOX = box(114.0, 4.0, 115.4, 5.1)
BRUNEI_MINERAL_NET_PER_YEAR = 107271.9

# Known reference totals for STP, summed over 2016-2024 (MgCO2e), validated
# externally. Hard-coded so the test never reads the reference dataset itself.
EXPECTED_TOTALS = {
    "gross_emissions_MgCO2e": 10426.5,
    "net_flux_MgCO2e": -11655664.0,
}


def test_stp_reproduces_reference_totals():
    datasets = stages.load_data(
        afolu_vegetation_zarr_uri,
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
        np.array([0, 1, 2, 3, 4]),
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

    assert set(country["carbon_pool"]) == {"vegetation"}

    # sign sanity: tree_gain is removals-only (net sink), tree_loss is a source
    by_cat = country.groupby("flux_class")[
        ["gross_emissions_MgCO2e", "net_flux_MgCO2e"]
    ].sum()
    assert by_cat.loc["tree_gain", "gross_emissions_MgCO2e"] == pytest.approx(0.0)
    assert by_cat.loc["tree_gain", "net_flux_MgCO2e"] < 0
    assert by_cat.loc["tree_loss", "net_flux_MgCO2e"] > 0


def test_brunei_mineral_soil_reproduces_reference():
    datasets = stages.load_soil_mineral(
        afolu_soc_zarr_uri,
        pixel_area_zarr_uri,
        country_zarr_uri,
        region_zarr_uri,
        subregion_zarr_uri,
        bbox=BRUNEI_BBOX,
    )
    expected_groups = (
        np.arange(999),
        np.arange(86),
        np.arange(854),
        np.array([1]),  # mineral
        np.arange(5),  # SOC interval index
    )
    cube, groupbys, out_expected_groups = stages.setup_soil_mineral_compute(
        datasets, expected_groups
    )
    reduced = common_stages.compute(cube, groupbys, out_expected_groups, "sum")
    df = stages.soil_mineral_result_dataframe(reduced)

    country = df[(df["aoi_id"] == "BRN") & (df["flux_class"] == "mineral")]
    assert set(country["carbon_pool"]) == {"soil"}
    # broadcast to every annual year -> constant net rate each year
    assert set(country["year"]) == set(range(2016, 2025))
    assert country["net_flux_MgCO2e"].to_numpy() == pytest.approx(
        BRUNEI_MINERAL_NET_PER_YEAR, rel=0.02
    )
