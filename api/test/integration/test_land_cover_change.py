from test.integration import (  # write_data_file,; write_metadata_file,
    delete_resource_files,
    retry_getting_resource,
)

import pandas as pd
import pytest
import pytest_asyncio
from app.domain.analyzers.land_cover_change_analyzer import LandCoverChangeAnalyzer
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.main import app
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.land_change.land_cover_change import LandCoverChangeAnalyticsIn
from app.routers.land_change.land_cover.land_cover_change import (
    ANALYTICS_NAME,
    create_analysis_service,
    get_analysis_repository,
)
from app.use_cases.analysis.analysis_service import AnalysisService
from asgi_lifespan import LifespanManager
from fastapi import Depends, Request
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

client = TestClient(app)


def get_file_system_analysis_repository() -> AnalysisRepository:
    return FileSystemAnalysisRepository(ANALYTICS_NAME)


def create_analysis_service_for_tests(
    request: Request,
    analysis_repository: AnalysisRepository = Depends(
        get_file_system_analysis_repository
    ),
) -> AnalysisService:
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=LandCoverChangeAnalyzer(
            analysis_repository=analysis_repository,
            compute_engine=request.app.state.dask_client,
        ),
        event=ANALYTICS_NAME,
    )


class TestLandCoverChangeData:
    @pytest_asyncio.fixture
    async def setup(self):
        """Runs before each test in this class"""
        analytics_in = LandCoverChangeAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["NGA.20.31"])
        )
        app.dependency_overrides[
            create_analysis_service
        ] = create_analysis_service_for_tests
        app.dependency_overrides[
            get_analysis_repository
        ] = get_file_system_analysis_repository
        delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                test_request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json=analytics_in.model_dump(),
                )

                yield test_request, client, analytics_in

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
        response = test_request
        assert response.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client, analysis_params = setup
        resource = await retry_getting_resource(
            ANALYTICS_NAME, analysis_params.thumbprint(), client
        )

        expected = pd.DataFrame(
            {
                "aoi_id": ["NGA.20.31", "NGA.20.31", "NGA.20.31"],
                "aoi_type": ["admin", "admin", "admin"],
                "land_cover_class_start": [
                    "Short vegetation",
                    "Short vegetation",
                    "Short vegetation",
                ],
                "land_cover_class_end": [
                    "Bare and sparse vegetation",
                    "Cropland",
                    "Built-up",
                ],
                "area_ha": [0.1505, 0.0752, 2.635],
            }
        )

        pd.testing.assert_frame_equal(
            pd.DataFrame(resource["result"]),
            expected,
            check_like=True,
            check_exact=False,  # Allow approximate comparison for numbers
            atol=1e-8,  # Absolute tolerance
            rtol=1e-2,  # Relative tolerance
        )
