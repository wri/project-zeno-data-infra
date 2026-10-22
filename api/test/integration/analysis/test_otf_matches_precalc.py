"""Checks that on-the-fly (OTF) results match precalculated admin results.

The same admin area is analyzed twice: as an admin AOI, which reads the
precalculated parquet built by the pipelines, and as its GADM polygon, which is
computed on the fly from the zarrs. The two paths share no analysis code, so if
OTF misaligns layers (e.g. filter layers not reindexed to the aggregate layer)
the results diverge.

Area matches the precalculated results to well under 0.1%: the pipelines assign
pixels to admin areas by rasterized GADM, which picks the same pixels as clipping
to the polygon. Carbon emissions differ by ~0.4% because the precalculated carbon
is built differently, so carbon is compared with a looser tolerance.
"""

import json
import os

import httpx
import pandas as pd
import pytest
import pytest_asyncio
from distributed import Client

from app.domain.analyzers import carbon_flux_analyzer as cf
from app.domain.analyzers import tree_cover_analyzer as tc
from app.domain.analyzers import tree_cover_gain_analyzer as tcg
from app.domain.analyzers import tree_cover_loss_analyzer as tcl
from app.domain.compute_engines.compute_engine import ComputeEngine
from app.domain.compute_engines.dask_client_router import DaskClientRouter
from app.domain.compute_engines.handlers.otf_implementations.flox_otf_handler import (
    FloxOTFHandler,
)
from app.domain.compute_engines.handlers.precalc_implementations.precalc_handlers import (  # noqa: E501
    TreeCoverGainPrecalcHandler,
    TreeCoverPrecalcHandler,
)
from app.domain.compute_engines.handlers.precalc_implementations.precalc_sql_query_builder import (  # noqa: E501
    PrecalcSqlQueryBuilder,
)
from app.domain.models.analysis import Analysis
from app.domain.models.environment import Environment
from app.domain.repositories.data_api_aoi_geometry_repository import (
    DataApiAoiGeometryRepository,
)
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    CustomAreaOfInterest,
)
from app.models.land_change.carbon_flux import CarbonFluxAnalyticsIn
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn
from app.models.land_change.tree_cover_gain import TreeCoverGainAnalyticsIn
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn

# Dumai, Riau, Indonesia: ~227k ha with forest, primary forest and loss in every
# year, small enough that each OTF run takes well under a minute.
ADMIN_ID = "IDN.24.2"
GADM_GID = "IDN.24.2_1"
# Same GADM version the pipelines rasterize for the precalculated results.
GADM_QUERY_URL = (
    "https://data-api.globalforestwatch.org/dataset/"
    "gadm_administrative_boundaries/v4.1.85/query"
)

AREA_RTOL = 1e-3
CARBON_RTOL = 1e-2
PROD = Environment.production

pytestmark = [pytest.mark.integration, pytest.mark.slow]


@pytest.fixture(scope="module")
def admin_aoi():
    return AdminAreaOfInterest(ids=[ADMIN_ID])


@pytest.fixture(scope="module")
def polygon_aoi():
    response = httpx.get(
        GADM_QUERY_URL,
        params={
            "sql": f"SELECT gfw_geojson FROM data "
            f"WHERE gid_2 = '{GADM_GID}' AND adm_level = '2'",
            "x-api-key": os.environ["API_KEY"],
        },
        follow_redirects=True,
        timeout=60,
    )
    geometry = response.json()["data"][0]["gfw_geojson"]

    return CustomAreaOfInterest(
        feature_collection={
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"id": ADMIN_ID},
                    "geometry": json.loads(geometry),
                }
            ],
        }
    )


@pytest_asyncio.fixture
async def dask_client(dask_test_cluster):
    async with Client(dask_test_cluster.scheduler_address, asynchronous=True) as c:
        yield c


def _otf_handler(dask_client):
    return FloxOTFHandler(
        dataset_repository=ZarrDatasetRepository(PROD),
        aoi_geometry_repository=DataApiAoiGeometryRepository(),
        dask_client_router=DaskClientRouter(dask_client, dask_client),
    )


def _precalc_engine(precalc_handler_cls, input_uris, dask_client):
    return ComputeEngine(
        handler=precalc_handler_cls(
            precalc_query_builder=PrecalcSqlQueryBuilder(),
            precalc_query_service=DuckDbPrecalcQueryService(
                table_uri=input_uris["admin_results_uri"]
            ),
            next_handler=_otf_handler(dask_client),
        )
    )


class _PolygonOTFEngine:
    """Computes the analyzer's query on the fly for the polygon AOI.

    Tree cover and tree cover gain don't accept feature collection AOIs yet, so
    their analyzers run with the admin AOI and this engine swaps in the polygon,
    which keeps the query exactly as the analyzer builds it.
    """

    def __init__(self, polygon_aoi, dask_client):
        self.polygon_aoi = polygon_aoi
        self.handler = _otf_handler(dask_client)

    async def compute(self, aoi, query):
        return await self.handler.handle(self.polygon_aoi, query)


async def _analyze(analyzer, analytics_in) -> pd.DataFrame:
    analysis = Analysis(None, analytics_in.model_dump(), AnalysisStatus.pending)
    await analyzer.analyze(analysis)
    return pd.DataFrame(analysis.result)


def _assert_matches(precalc, otf, group_by, columns, rtol):
    if group_by:
        precalc = precalc.groupby(group_by)[columns].sum()
        otf = otf.groupby(group_by)[columns].sum()
        assert sorted(otf.index) == sorted(precalc.index)
        otf = otf.loc[precalc.index]
    else:
        precalc = precalc[columns].sum()
        otf = otf[columns].sum()

    assert precalc.to_numpy().sum() > 0
    assert otf.to_numpy().flatten() == pytest.approx(
        precalc.to_numpy().flatten(), rel=rtol
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "forest_filter,start_year",
    [(None, "2001"), ("primary_forest", "2001"), ("natural_forest_only", "2021")],
)
async def test_tree_cover_loss(
    forest_filter, start_year, admin_aoi, polygon_aoi, dask_client
):
    analyzer = tcl.TreeCoverLossAnalyzer(
        dask_client_router=DaskClientRouter(dask_client, dask_client),
        dataset_repository=ZarrDatasetRepository(PROD),
        aoi_geometry_repository=DataApiAoiGeometryRepository(),
        input_uris=tcl.INPUT_URIS[PROD],
    )
    params = dict(
        start_year=start_year,
        end_year="2025",
        canopy_cover=30,
        forest_filter=forest_filter,
        intersections=[],
    )

    precalc = await _analyze(
        analyzer, TreeCoverLossAnalyticsIn(aoi=admin_aoi, **params)
    )
    otf = await _analyze(analyzer, TreeCoverLossAnalyticsIn(aoi=polygon_aoi, **params))

    group_by = ["tree_cover_loss_year"]
    _assert_matches(precalc, otf, group_by, ["area_ha"], AREA_RTOL)
    _assert_matches(precalc, otf, group_by, ["carbon_emissions_MgCO2e"], CARBON_RTOL)


@pytest.mark.asyncio
async def test_tree_cover(admin_aoi, polygon_aoi, dask_client):
    input_uris = tc.INPUT_URIS[PROD]
    analytics_in = TreeCoverAnalyticsIn(aoi=admin_aoi, canopy_cover=30)

    precalc = await _analyze(
        tc.TreeCoverAnalyzer(
            compute_engine=_precalc_engine(
                TreeCoverPrecalcHandler, input_uris, dask_client
            ),
            input_uris=input_uris,
        ),
        analytics_in,
    )
    otf = await _analyze(
        tc.TreeCoverAnalyzer(
            compute_engine=_PolygonOTFEngine(polygon_aoi, dask_client),
            input_uris=input_uris,
        ),
        analytics_in,
    )

    _assert_matches(precalc, otf, [], ["area_ha"], AREA_RTOL)


@pytest.mark.asyncio
async def test_tree_cover_gain(admin_aoi, polygon_aoi, dask_client):
    input_uris = tcg.INPUT_URIS[PROD]
    analytics_in = TreeCoverGainAnalyticsIn(
        aoi=admin_aoi, start_year="2000", end_year="2020"
    )

    precalc = await _analyze(
        tcg.TreeCoverGainAnalyzer(
            compute_engine=_precalc_engine(
                TreeCoverGainPrecalcHandler, input_uris, dask_client
            ),
            input_uris=input_uris,
        ),
        analytics_in,
    )
    otf = await _analyze(
        tcg.TreeCoverGainAnalyzer(
            compute_engine=_PolygonOTFEngine(polygon_aoi, dask_client),
            input_uris=input_uris,
        ),
        analytics_in,
    )

    # Gain differs by up to ~0.1% per period, a little more than other areas.
    _assert_matches(precalc, otf, ["tree_cover_gain_period"], ["area_ha"], 5e-3)


@pytest.mark.asyncio
async def test_carbon_flux(admin_aoi, polygon_aoi, dask_client):
    input_uris = cf.INPUT_URIS[PROD]
    analyzer = cf.CarbonFluxAnalyzer(
        compute_engine=dask_client,
        query_service=DuckDbPrecalcQueryService(
            table_uri=input_uris["admin_results_uri"]
        ),
        input_uris=input_uris,
    )

    precalc = await _analyze(
        analyzer, CarbonFluxAnalyticsIn(aoi=admin_aoi, canopy_cover=30)
    )
    otf = await _analyze(
        analyzer, CarbonFluxAnalyticsIn(aoi=polygon_aoi, canopy_cover=30)
    )

    _assert_matches(
        precalc,
        otf,
        [],
        [
            "carbon_net_flux_Mg_CO2e",
            "carbon_gross_removals_Mg_CO2e",
            "carbon_gross_emissions_Mg_CO2e",
        ],
        CARBON_RTOL,
    )
