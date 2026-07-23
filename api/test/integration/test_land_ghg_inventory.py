from test.integration import (
    delete_resource_files,
    resource_thumbprint,
)

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import Depends
from httpx import ASGITransport, AsyncClient

from app.domain.analyzers.land_ghg_inventory_analyzer import (
    INPUT_URIS,
    LandGHGInventoryAnalyzer,
)
from app.domain.models.environment import Environment
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.main import app
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.land_change.land_ghg_inventory import (
    ANALYTICS_NAME,
    LandGHGInventoryAnalyticsIn,
)
from app.routers.land_change.land_ghg_inventory.land_ghg_inventory import (
    create_analysis_service,
    get_analysis_repository,
)
from app.use_cases.analysis.analysis_service import AnalysisService


def get_file_system_analysis_repository() -> AnalysisRepository:
    return FileSystemAnalysisRepository(ANALYTICS_NAME)


def create_analysis_service_for_tests(
    analysis_repository: AnalysisRepository = Depends(
        get_file_system_analysis_repository
    ),
) -> AnalysisService:
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=LandGHGInventoryAnalyzer(
            input_uris=INPUT_URIS[Environment.production]
        ),
        event=ANALYTICS_NAME,
    )


class TestLandGHGInventoryPostWithNoPreviousRequest:
    @pytest_asyncio.fixture
    async def setup(self):
        analytics_in = LandGHGInventoryAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["BRA.1"])
        )
        analyzer = LandGHGInventoryAnalyzer(
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
                yield test_request, resource_tp

        app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted(self, setup):
        test_request, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, setup):
        test_request, _ = setup
        assert test_request.json()["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, resource_tp = setup
        assert test_request.json()["data"]["link"] == (
            f"http://testserver/v0/land_change/{ANALYTICS_NAME}/analytics/{resource_tp}"
        )


def test_endpoint_is_hidden_from_openapi_schema_but_registered():
    # hidden from the public docs...
    schema = app.openapi()
    assert not any(ANALYTICS_NAME in path for path in schema["paths"])
    # ...but the routes still exist and are callable
    registered = {getattr(route, "path", "") for route in app.routes}
    assert f"/v0/land_change/{ANALYTICS_NAME}/analytics" in registered
    assert f"/v0/land_change/{ANALYTICS_NAME}/analytics/{{resource_id}}" in registered
