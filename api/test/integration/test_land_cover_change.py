from test.integration import (
    delete_resource_files,
    resource_thumbprint,
    retry_getting_resource,
)
from unittest.mock import patch

import pandas as pd
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import Depends, Header, Request
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

from app.dependencies import get_environment
from app.domain.analyzers.land_cover_change_analyzer import (
    INPUT_URIS,
    LandCoverChangeAnalyzer,
)
from app.domain.models.environment import Environment
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.main import app
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.land_change.land_cover_change import (
    ANALYTICS_NAME,
    LandCoverChangeAnalyticsIn,
)
from app.routers.land_change.land_cover.land_cover_change import (
    create_analysis_service,
    get_analysis_repository,
)
from app.use_cases.analysis.analysis_service import AnalysisService

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
            compute_engine=request.app.state.dask_client,
            query_service=DuckDbPrecalcQueryService(
                table_uri=INPUT_URIS[Environment.production]["admin_results_uri"],
            ),
            input_uris=INPUT_URIS[Environment.production],
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
        analyzer = LandCoverChangeAnalyzer(
            input_uris=INPUT_URIS[Environment.production]
        )
        resource_tp = resource_thumbprint(analytics_in, analyzer)

        app.dependency_overrides[create_analysis_service] = (
            create_analysis_service_for_tests
        )
        app.dependency_overrides[get_analysis_repository] = (
            get_file_system_analysis_repository
        )
        delete_resource_files(ANALYTICS_NAME, resource_tp)

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                test_request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json=analytics_in.model_dump(),
                )

                yield test_request, client, resource_tp
        app.dependency_overrides.clear()

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
        response = test_request
        assert response.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client, resource_tp = setup
        resource = await retry_getting_resource(ANALYTICS_NAME, resource_tp, client)

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


@pytest.mark.asyncio
async def test_staging_env_header_is_forwarded_to_service():
    # Regression test for bug fixed in
    # https://github.com/wri/project-zeno-data-infra/pull/244
    resolved_environments = []

    async def tracking_get_environment(
        x_environment: Environment | None = Header(default=None),
    ) -> Environment:
        resolved = (
            x_environment if x_environment is not None else Environment.production
        )
        resolved_environments.append(resolved)
        return resolved

    app.dependency_overrides.clear()
    app.dependency_overrides[get_environment] = tracking_get_environment
    app.dependency_overrides[get_analysis_repository] = (
        get_file_system_analysis_repository
    )

    analytics_in = LandCoverChangeAnalyticsIn(
        aoi=AdminAreaOfInterest(type="admin", ids=["NGA.20.31"])
    )

    patched_uris = {
        **INPUT_URIS,
        Environment.staging: INPUT_URIS[Environment.production],
    }

    try:
        with patch(
            "app.routers.land_change.land_cover.land_cover_change.INPUT_URIS",
            patched_uris,
        ):
            async with LifespanManager(app):
                async with AsyncClient(
                    transport=ASGITransport(app), base_url="http://testserver"
                ) as client:
                    response = await client.post(
                        f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                        json=analytics_in.model_dump(),
                        headers={"x-environment": "staging"},
                    )
    finally:
        app.dependency_overrides.clear()

    assert resolved_environments == [Environment.staging], (
        f"Expected get_environment to be called with staging, got: {resolved_environments}. "
        "If this list is empty, the dependency is likely wired as Depends(Environment) "
        "instead of Depends(get_environment), causing the override to be silently ignored."
    )
    assert response.status_code == 202
