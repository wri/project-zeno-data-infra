"""Checks that on-the-fly (OTF) results match precalculated admin results.

An admin area's GADM polygon is analyzed on the fly from the zarrs and compared
with the precalculated results for that admin area, stored below. The two are
produced independently from the same inputs, so they should agree. Tree cover
loss with primary_forest is used because it combines filtering, grouping and
aggregation across several layers.
"""

import json
from pathlib import Path

import pandas as pd
import pytest
import pytest_asyncio
from distributed import Client

from app.domain.analyzers.tree_cover_loss_analyzer import (
    INPUT_URIS,
    TreeCoverLossAnalyzer,
)
from app.domain.compute_engines.dask_client_router import DaskClientRouter
from app.domain.models.analysis import Analysis
from app.domain.models.environment import Environment
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import CustomAreaOfInterest
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn

# Dumai, Riau, Indonesia (IDN.24.2): ~227k ha with primary forest loss in every
# year. The polygon is GADM v4.1.85, the version the pipelines rasterize to
# assign pixels to admin areas for the precalculated results.
POLYGON_FIXTURE = Path(__file__).parent / "fixtures" / "gadm_IDN.24.2.geojson"

# Precalculated primary forest loss (ha) for IDN.24.2 at canopy cover >= 30, from
# the admin results parquet in INPUT_URIS. Update if the TCL inputs change.
EXPECTED_AREA_HA = {
    2001: 655.935909,
    2002: 1233.688142,
    2003: 1082.832033,
    2004: 1804.868069,
    2005: 6252.677689,
    2006: 7533.303521,
    2007: 3312.287429,
    2008: 2422.202515,
    2009: 8360.356534,
    2010: 11466.664349,
    2011: 6234.917264,
    2012: 8130.680192,
    2013: 4779.286942,
    2014: 7488.371459,
    2015: 2243.708344,
    2016: 2719.486104,
    2017: 2362.260073,
    2018: 2287.533050,
    2019: 883.442821,
    2020: 1405.027477,
    2021: 888.127668,
    2022: 1185.201564,
    2023: 1430.206909,
    2024: 2647.067236,
    2025: 1672.815301,
}
EXPECTED_CARBON_EMISSIONS_MGCO2E = 113_880_595.26

PROD = Environment.production

pytestmark = [pytest.mark.integration, pytest.mark.slow]


@pytest_asyncio.fixture
async def dask_client(dask_test_cluster):
    async with Client(dask_test_cluster.scheduler_address, asynchronous=True) as c:
        yield c


@pytest.mark.asyncio
async def test_otf_tree_cover_loss_matches_precalc(dask_client):
    analyzer = TreeCoverLossAnalyzer(
        dask_client_router=DaskClientRouter(dask_client, dask_client),
        dataset_repository=ZarrDatasetRepository(PROD),
        input_uris=INPUT_URIS[PROD],
    )
    analytics_in = TreeCoverLossAnalyticsIn(
        aoi=CustomAreaOfInterest(
            feature_collection=json.loads(POLYGON_FIXTURE.read_text())
        ),
        start_year="2001",
        end_year="2025",
        canopy_cover=30,
        forest_filter="primary_forest",
        intersections=[],
    )

    analysis = Analysis(None, analytics_in.model_dump(), AnalysisStatus.pending)
    await analyzer.analyze(analysis)
    results = pd.DataFrame(analysis.result).set_index("tree_cover_loss_year")

    assert sorted(results.index) == sorted(EXPECTED_AREA_HA)
    # Rasterized GADM picks the same pixels as clipping to the polygon, so area
    # matches the precalculated results closely in every year.
    assert results.area_ha[list(EXPECTED_AREA_HA)].tolist() == pytest.approx(
        list(EXPECTED_AREA_HA.values()), rel=1e-3
    )
    # Precalculated carbon is built differently, which can vary by year, so
    # carbon is compared on the total.
    assert results.carbon_emissions_MgCO2e.sum() == pytest.approx(
        EXPECTED_CARBON_EMISSIONS_MGCO2E, rel=1e-2
    )
