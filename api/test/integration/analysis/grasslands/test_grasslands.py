from test.integration import delete_resource_files, retry_getting_resource

import pandas as pd
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import Depends, Request
from httpx import ASGITransport, AsyncClient

from app.domain.analyzers.grasslands_analyzer import INPUT_URIS, GrasslandsAnalyzer
from app.domain.models.environment import Environment
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
from app.models.land_change.grasslands import ANALYTICS_NAME, GrasslandsAnalyticsIn
from app.routers.land_change.grasslands.grasslands import (
    create_analysis_service,
    get_analysis_repository,
)
from app.use_cases.analysis.analysis_service import AnalysisService, resource_thumbprint


def get_file_system_analysis_repository():
    return FileSystemAnalysisRepository(ANALYTICS_NAME)


def create_analysis_service_for_tests(
    request: Request, analysis_repository=Depends(get_file_system_analysis_repository)
) -> AnalysisService:
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=GrasslandsAnalyzer(
            compute_engine=request.app.state.dask_client,
            duckdb_query_service=DuckDbPrecalcQueryService(
                table_uri=INPUT_URIS[Environment.production]["admin_results_table_uri"]
            ),
            input_uris=INPUT_URIS[Environment.production],
        ),
        event=ANALYTICS_NAME,
    )


class TestAnalyticsPostWithMultipleAdminAOIs:
    @pytest_asyncio.fixture
    async def setup(self):
        analytics_in = GrasslandsAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9", "BRA.14"]),
            start_year="2015",
            end_year="2020",
        )
        analyzer = GrasslandsAnalyzer(input_uris=INPUT_URIS[Environment.production])
        resource_tp = resource_thumbprint(analytics_in, analyzer)

        delete_resource_files(ANALYTICS_NAME, resource_tp)

        app.dependency_overrides[create_analysis_service] = (
            create_analysis_service_for_tests
        )
        app.dependency_overrides[get_analysis_repository] = (
            get_file_system_analysis_repository
        )

        try:
            async with LifespanManager(app):
                async with AsyncClient(
                    transport=ASGITransport(app), base_url="http://testserver"
                ) as client:
                    request = await client.post(
                        f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                        json=analytics_in.model_dump(),
                    )

                    yield request, client, resource_tp
        finally:
            app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, setup):
        test_request, _, _ = setup
        resource = test_request.json()
        assert resource["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, _, resource_tp = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == f"http://testserver/v0/land_change/{ANALYTICS_NAME}/analytics/{resource_tp}"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        _, client, resource_tp = setup
        data = await retry_getting_resource(ANALYTICS_NAME, resource_tp, client)

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert "IDN.24.9" in df["aoi_id"].values
        assert "BRA.14" in df["aoi_id"].values

        assert df.area_ha.any()
        assert df.columns.size == 4  # aoi_id, aoi_type, area_ha, year


class TestGrasslandsAnalyticsPostWithKba:
    @pytest_asyncio.fixture
    async def setup(self):
        analytics_in = GrasslandsAnalyticsIn(
            aoi=KeyBiodiversityAreaOfInterest(
                type="key_biodiversity_area", ids=["20401", "19426"]
            ),
            start_year="2015",
            end_year="2020",
        )
        analyzer = GrasslandsAnalyzer(input_uris=INPUT_URIS[Environment.production])
        resource_tp = resource_thumbprint(analytics_in, analyzer)

        delete_resource_files(ANALYTICS_NAME, resource_tp)

        app.dependency_overrides[create_analysis_service] = (
            create_analysis_service_for_tests
        )
        app.dependency_overrides[get_analysis_repository] = (
            get_file_system_analysis_repository
        )

        try:
            async with LifespanManager(app):
                async with AsyncClient(
                    transport=ASGITransport(app), base_url="http://testserver"
                ) as client:
                    request = await client.post(
                        f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                        json=analytics_in.model_dump(),
                    )

                    yield request, client, resource_tp
        finally:
            app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, setup):
        test_request, _, _ = setup
        resource = test_request.json()
        assert resource["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, _, resource_tp = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == f"http://testserver/v0/land_change/{ANALYTICS_NAME}/analytics/{resource_tp}"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        _, client, resource_tp = setup
        data = await retry_getting_resource(ANALYTICS_NAME, resource_tp, client)

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert df.area_ha.any()
        assert df.columns.size == 4  # aoi_id, aoi_type, area_ha, year
