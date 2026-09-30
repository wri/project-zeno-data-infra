from test.integration import (
    delete_resource_files,
    resource_thumbprint,
    retry_getting_resource,
)
from typing import Dict

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import Depends
from httpx import ASGITransport, AsyncClient

from app.authentication import get_authenticator
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
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    GlobalAreaOfInterest,
)
from app.models.common.authentication import User
from app.models.land_change.land_ghg_inventory import (
    ANALYTICS_NAME,
    LandGHGInventoryAnalyticsIn,
)
from app.routers.land_change.land_ghg_inventory.land_ghg_inventory import (
    create_analysis_service,
    get_analysis_repository,
)
from app.use_cases.analysis.analysis_service import AnalysisService


class FakeQueryService:
    """Stand-in for a precomputed parquet so the full POST -> background -> GET
    flow runs without touching S3. Returns a fixed column-oriented payload."""

    def __init__(self, payload: Dict):
        self.payload = payload

    async def execute(self, query: str) -> Dict:
        return self.payload


VEGETATION_PAYLOAD = {
    "aoi_id": ["BRA.1", "BRA.1"],
    # Every payload here carries aoi_type because the analyzer selects it as a
    # SQL literal rather than reading it from the table, and this fake stands in
    # for the query's result.
    "aoi_type": ["admin", "admin"],
    "land_state_class": ["tree_loss", "tree_gain"],
    "year": [2016, 2016],
    "gross_emissions_MgCO2e": [100.0, 0.0],
    "gross_removals_MgCO2": [-10.0, -20.0],
    "net_flux_MgCO2e": [90.0, -20.0],
    "area_ha": [1.0, 2.0],
}

AGRICULTURE_PAYLOAD = {
    "aoi_id": ["BRA.1", "BRA.1"],
    "aoi_type": ["admin", "admin"],
    "category": ["cropland", "livestock"],
    "gross_emissions_MgCO2e": [123.0, 45.0],
}

MINERAL_SOIL_PAYLOAD = {
    "aoi_id": ["BRA.1"],
    "aoi_type": ["admin"],
    "gross_emissions_MgCO2e": [100.0],
    "gross_removals_MgCO2": [-10.0],
    "net_flux_MgCO2e": [90.0],
    "area_ha": [1.0],
}

ORGANIC_SOIL_PAYLOAD = {
    "aoi_id": ["BRA.1", "BRA.1"],
    "aoi_type": ["admin", "admin"],
    "interval_end_year": [2020, 2024],
    "gross_emissions_MgCO2e": [10.0, 20.0],
    "area_ha": [1.0, 1.0],
}


class FakeAdminAuthenticator:
    """Authenticates any token as a ResourceWatch admin, so the flow tests focus
    on behavior rather than a live RW call."""

    async def get_user(self, token):
        return User(id="admin", role="ADMIN", extraUserData={})


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
            query_services={
                "vegetation": FakeQueryService(VEGETATION_PAYLOAD),
                "agriculture": FakeQueryService(AGRICULTURE_PAYLOAD),
                "mineral_soil": FakeQueryService(MINERAL_SOIL_PAYLOAD),
                "organic_soil": FakeQueryService(ORGANIC_SOIL_PAYLOAD),
            },
            input_uris=INPUT_URIS[Environment.production],
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
        app.dependency_overrides[get_authenticator] = FakeAdminAuthenticator
        delete_resource_files(ANALYTICS_NAME, resource_tp)

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app),
                base_url="http://testserver",
                headers={"Authorization": "Bearer test-admin"},
            ) as client:
                test_request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json=analytics_in.model_dump(),
                )
                yield test_request, client, resource_tp

        app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted(self, setup):
        test_request, _, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, setup):
        test_request, _, _ = setup
        assert test_request.json()["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, _, resource_tp = setup
        assert test_request.json()["data"]["link"] == (
            f"http://testserver/v0/land_change/{ANALYTICS_NAME}/analytics/{resource_tp}"
        )

    @pytest.mark.asyncio
    async def test_get_returns_saved_result(self, setup):
        _, client, resource_tp = setup

        data = await retry_getting_resource(ANALYTICS_NAME, resource_tp, client)

        assert data["status"] == "saved"
        # one table per category
        assert set(data["result"]) == {
            "vegetation",
            "agriculture",
            "mineral_soil",
            "organic_soil",
        }

        vegetation = data["result"]["vegetation"]
        assert set(vegetation).issuperset(
            {
                "aoi_id",
                "aoi_type",
                "land_state_class",
                "year",
                "gross_emissions_MgCO2e",
                "gross_removals_MgCO2",
                "net_flux_MgCO2e",
                "area_ha",
            }
        )
        assert set(vegetation["aoi_type"]) == {"admin"}

        agriculture = data["result"]["agriculture"]
        assert set(agriculture).issuperset(
            {"aoi_id", "aoi_type", "category", "year", "gross_emissions_MgCO2e"}
        )
        assert set(agriculture["category"]) == {"cropland", "livestock"}
        # the static snapshot is broadcast across every vegetation year, per category
        assert set(agriculture["year"]) == set(range(2016, 2025))

        mineral_soil = data["result"]["mineral_soil"]
        assert set(mineral_soil).issuperset(
            {
                "aoi_id",
                "aoi_type",
                "year",
                "gross_emissions_MgCO2e",
                "gross_removals_MgCO2",
                "net_flux_MgCO2e",
                "area_ha",
            }
        )
        assert "interval_end_year" not in mineral_soil
        # the static snapshot is broadcast across every vegetation year
        assert set(mineral_soil["year"]) == set(range(2016, 2025))

        organic_soil = data["result"]["organic_soil"]
        assert set(organic_soil).issuperset(
            {
                "aoi_id",
                "aoi_type",
                "year",
                "gross_emissions_MgCO2e",
                "area_ha",
            }
        )
        assert "interval_end_year" not in organic_soil
        # both blocks are broadcast across every vegetation year
        assert set(organic_soil["year"]) == set(range(2016, 2025))


class GlobalAggregateQueryService:
    """Stand-in for a precomputed parquet on the global path.

    Unlike FakeQueryService it inspects the SQL: the point of a global analysis
    is the aggregate, so a query that forgot to restrict to the country tier or
    to sum would quietly produce a plausible-looking result here. Returns the
    world row the real query would.
    """

    def __init__(self, payload: Dict):
        self.payload = payload

    async def execute(self, query: str) -> Dict:
        assert "not like '%.%'" in query, f"global query must filter tiers: {query}"
        assert "sum(" in query, f"global query must aggregate: {query}"
        return self.payload


GLOBAL_VEGETATION_PAYLOAD = {
    "aoi_id": ["GLOBAL"],
    "aoi_type": ["global"],
    "land_state_class": ["tree_loss"],
    "year": [2016],
    "gross_emissions_MgCO2e": [150.0],
    "gross_removals_MgCO2": [-15.0],
    "net_flux_MgCO2e": [135.0],
    "area_ha": [12.0],
}

GLOBAL_AGRICULTURE_PAYLOAD = {
    "aoi_id": ["GLOBAL", "GLOBAL"],
    "aoi_type": ["global", "global"],
    "category": ["cropland", "livestock"],
    "gross_emissions_MgCO2e": [150.0, 150.0],
}

GLOBAL_MINERAL_SOIL_PAYLOAD = {
    "aoi_id": ["GLOBAL"],
    "aoi_type": ["global"],
    "gross_emissions_MgCO2e": [150.0],
    "gross_removals_MgCO2": [-15.0],
    "net_flux_MgCO2e": [135.0],
    "area_ha": [12.0],
}

GLOBAL_ORGANIC_SOIL_PAYLOAD = {
    "aoi_id": ["GLOBAL", "GLOBAL"],
    "aoi_type": ["global", "global"],
    "interval_end_year": [2020, 2024],
    "gross_emissions_MgCO2e": [150.0, 300.0],
    "area_ha": [12.0, 12.0],
}


def create_global_analysis_service_for_tests(
    analysis_repository: AnalysisRepository = Depends(
        get_file_system_analysis_repository
    ),
) -> AnalysisService:
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=LandGHGInventoryAnalyzer(
            query_services={
                "vegetation": GlobalAggregateQueryService(GLOBAL_VEGETATION_PAYLOAD),
                "agriculture": GlobalAggregateQueryService(GLOBAL_AGRICULTURE_PAYLOAD),
                "mineral_soil": GlobalAggregateQueryService(
                    GLOBAL_MINERAL_SOIL_PAYLOAD
                ),
                "organic_soil": GlobalAggregateQueryService(
                    GLOBAL_ORGANIC_SOIL_PAYLOAD
                ),
            },
            input_uris=INPUT_URIS[Environment.production],
        ),
        event=ANALYTICS_NAME,
    )


class TestLandGHGInventoryGlobalAoi:
    """The whole world as one AOI: same endpoint, same four tables, but a
    single world row set instead of one row per admin area."""

    @pytest_asyncio.fixture
    async def setup(self):
        analytics_in = LandGHGInventoryAnalyticsIn(aoi=GlobalAreaOfInterest())
        analyzer = LandGHGInventoryAnalyzer(
            input_uris=INPUT_URIS[Environment.production]
        )
        resource_tp = resource_thumbprint(analytics_in, analyzer)

        app.dependency_overrides[create_analysis_service] = (
            create_global_analysis_service_for_tests
        )
        app.dependency_overrides[get_analysis_repository] = (
            get_file_system_analysis_repository
        )
        app.dependency_overrides[get_authenticator] = FakeAdminAuthenticator
        delete_resource_files(ANALYTICS_NAME, resource_tp)

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app),
                base_url="http://testserver",
                headers={"Authorization": "Bearer test-admin"},
            ) as client:
                test_request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json={"aoi": {"type": "global"}},
                )
                yield test_request, client, resource_tp

        app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted(self, setup):
        test_request, _, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_get_returns_one_world_row_set(self, setup):
        _, client, resource_tp = setup

        data = await retry_getting_resource(ANALYTICS_NAME, resource_tp, client)

        assert data["status"] == "saved"
        assert set(data["result"]) == {
            "vegetation",
            "agriculture",
            "mineral_soil",
            "organic_soil",
        }

        # Every table is labelled as the world, not as an admin area.
        for table in data["result"].values():
            assert set(table["aoi_id"]) == {"GLOBAL"}
            assert set(table["aoi_type"]) == {"global"}

        # The aggregate keeps each component's own dimensions rather than
        # collapsing to a single number.
        agriculture = data["result"]["agriculture"]
        assert set(agriculture["category"]) == {"cropland", "livestock"}
        assert set(agriculture["year"]) == set(range(2016, 2025))
        # One world total per category, repeated per year -- not multiplied by
        # the number of years.
        assert set(agriculture["gross_emissions_MgCO2e"]) == {150.0}

        organic_soil = data["result"]["organic_soil"]
        assert "interval_end_year" not in organic_soil
        assert set(organic_soil["year"]) == set(range(2016, 2025))

    @pytest.mark.asyncio
    async def test_metadata_records_the_global_aoi(self, setup):
        _, client, resource_tp = setup

        data = await retry_getting_resource(ANALYTICS_NAME, resource_tp, client)

        assert data["metadata"]["aoi"] == {"type": "global"}


@pytest.mark.asyncio
async def test_global_and_admin_requests_get_separate_resources():
    # The thumbprint already separates the two, which is why _version does not
    # need bumping for the global AOI.
    analyzer = LandGHGInventoryAnalyzer(input_uris=INPUT_URIS[Environment.production])
    admin_tp = resource_thumbprint(
        LandGHGInventoryAnalyticsIn(aoi=AdminAreaOfInterest(ids=["BRA"])), analyzer
    )
    global_tp = resource_thumbprint(
        LandGHGInventoryAnalyticsIn(aoi=GlobalAreaOfInterest()), analyzer
    )

    assert admin_tp != global_tp


@pytest.mark.asyncio
async def test_requests_without_a_token_are_unauthorized():
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
                response = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json={"aoi": {"type": "admin", "ids": ["BRA.1"]}},
                )
        assert response.status_code == 401
    finally:
        app.dependency_overrides.clear()


def test_endpoint_is_hidden_from_openapi_schema_but_registered():
    # hidden from the public docs...
    schema = app.openapi()
    assert not any(ANALYTICS_NAME in path for path in schema["paths"])
    # ...but the routes still exist and are callable
    registered = {getattr(route, "path", "") for route in app.routes}
    assert f"/v0/land_change/{ANALYTICS_NAME}/analytics" in registered
    assert f"/v0/land_change/{ANALYTICS_NAME}/analytics/{{resource_id}}" in registered
