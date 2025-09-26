from test.integration import delete_resource_files, retry_getting_resource

import pandas as pd
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import Depends, Request
from httpx import ASGITransport, AsyncClient

from app.domain.analyzers.tree_cover_analyzer import TreeCoverAnalyzer
from app.domain.compute_engines.compute_engine import ComputeEngine
from app.domain.compute_engines.handlers.otf_implementations.flox_otf_handler import (
    FloxOTFHandler,
)
from app.domain.compute_engines.handlers.precalc_implementations.precalc_handlers import (
    TreeCoverPrecalcHandler,
)
from app.domain.compute_engines.handlers.precalc_implementations.precalc_sql_query_builder import (
    PrecalcSqlQueryBuilder,
)
from app.domain.repositories.data_api_aoi_geometry_repository import (
    DataApiAoiGeometryRepository,
)
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.main import app
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
)
from app.models.land_change.tree_cover import ANALYTICS_NAME, TreeCoverAnalyticsIn
from app.routers.land_change.tree_cover.tree_cover import (
    create_analysis_service,
    get_analysis_repository,
)
from app.use_cases.analysis.analysis_service import AnalysisService


def get_file_system_analysis_repository():
    return FileSystemAnalysisRepository(ANALYTICS_NAME)


def create_analysis_service_for_tests(
    request: Request, analysis_repository=Depends(get_file_system_analysis_repository)
) -> AnalysisService:
    compute_engine = ComputeEngine(
        handler=TreeCoverPrecalcHandler(
            precalc_query_builder=PrecalcSqlQueryBuilder(),
            precalc_query_service=DuckDbPrecalcQueryService(
                table_uri="s3://lcl-analytics/zonal-statistics/admin-tree-cover.parquet"
            ),
            next_handler=FloxOTFHandler(
                dataset_repository=ZarrDatasetRepository(),
                aoi_geometry_repository=DataApiAoiGeometryRepository(),
                dask_client=request.app.state.dask_client,
            ),
        )
    )

    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=TreeCoverAnalyzer(compute_engine=compute_engine),
        event=ANALYTICS_NAME,
    )


class TestAnalyticsPostWithMultipleAdminAOIs:
    @pytest_asyncio.fixture()
    async def setup(self):
        analytics_in = TreeCoverAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9", "BRA.14"]),
            canopy_cover=15,
        )
        app.dependency_overrides[create_analysis_service] = (
            create_analysis_service_for_tests
        )
        app.dependency_overrides[get_analysis_repository] = (
            get_file_system_analysis_repository
        )
        delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json=analytics_in.model_dump(),
                )

                yield (request, client, analytics_in)

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, setup):
        test_request, _, _ = setup
        resource = test_request.json()
        assert resource["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, _, analysis_params = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == f"http://testserver/v0/land_change/{ANALYTICS_NAME}/analytics/{analysis_params.thumbprint()}"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client, analysis_params = setup
        data = await retry_getting_resource(
            ANALYTICS_NAME, analysis_params.thumbprint(), client
        )

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert "IDN.24.9" in df["aoi_id"].values
        assert "BRA.14" in df["aoi_id"].values

        assert df.area_ha.any()
        assert df.columns.size == 3  # aoi_id, aoi_type, area_ha


class TestTreeCoverAnalyticsPostWithKba:
    @pytest_asyncio.fixture()
    async def setup(self):
        analytics_in = TreeCoverAnalyticsIn(
            aoi=KeyBiodiversityAreaOfInterest(
                type="key_biodiversity_area", ids=["20401", "19426"]
            ),
            canopy_cover=15,
        )
        app.dependency_overrides[create_analysis_service] = (
            create_analysis_service_for_tests
        )
        app.dependency_overrides[get_analysis_repository] = (
            get_file_system_analysis_repository
        )
        delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json=analytics_in.model_dump(),
                )

                yield (request, client, analytics_in)

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, setup):
        test_request, _, _ = setup
        resource = test_request.json()
        assert resource["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, _, analysis_params = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == f"http://testserver/v0/land_change/{ANALYTICS_NAME}/analytics/{analysis_params.thumbprint()}"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client, analysis_params = setup
        data = await retry_getting_resource(
            ANALYTICS_NAME, analysis_params.thumbprint(), client
        )

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert df.area_ha.any()
        assert df.columns.size == 3  # aoi_id, aoi_type, area_ha
