from test.integration import delete_resource_files, retry_getting_resource

import pandas as pd
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import Depends, Request
from httpx import ASGITransport, AsyncClient

from api.app.domain.analyzers.deforestation_luc_emissions_factor_analyzer import (
    DeforestationLUCEmissionsFactorAnalyzer,
)
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.main import app
from app.models.land_change.deforestation_luc_emissions_factor import (
    ANALYTICS_NAME,
    DeforestationLUCEmissionsFactorAnalyticsIn,
)
from app.routers.land_change.deforestation_luc_emissions_factor.deforestation_luc_emissions_factor import (
    create_analysis_service,
    get_analysis_repository,
)
from app.use_cases.analysis.analysis_service import AnalysisService


def get_file_system_analysis_repository():
    return FileSystemAnalysisRepository(ANALYTICS_NAME)


def create_analysis_service_for_tests(
    request: Request, analysis_repository=Depends(get_file_system_analysis_repository)
) -> AnalysisService:
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=DeforestationLUCEmissionsFactorAnalyzer(
            analysis_repository=analysis_repository,
            compute_engine=request.app.state.dask_client,
            query_service=DuckDbPrecalcQueryService(
                table_uri="s3://lcl-analytics/zonal-statistics/admin-deforestation-luc-emissions-factor.parquet"
            ),
        ),
        event=ANALYTICS_NAME,
    )


class TestAnalyticsPostWithMultipleAdminAOIs:
    @pytest_asyncio.fixture
    async def setup(self):
        analytics_in = DeforestationLUCEmissionsFactorAnalyticsIn(
            aoi={"type": "admin", "ids": ["BRA.14", "IDN.24.9"]},
            gas_types=["CO2e", "CH4"],
            crop_types=["Banana"],
            start_year="2021",
            end_year="2023",
        )

        delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())

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
                        json=analytics_in.model_dump(mode="json"),
                    )

                    yield request, client, analytics_in
        finally:
            app.dependency_overrides.clear()

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

        assert df.columns.size == 8
