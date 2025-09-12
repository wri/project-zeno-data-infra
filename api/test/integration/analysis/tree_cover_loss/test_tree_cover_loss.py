from test.integration import delete_resource_files, retry_getting_resource

import pandas as pd
import pytest
import pytest_asyncio
from app.domain.analyzers.tree_cover_loss_analyzer import TreeCoverLossAnalyzer
from app.domain.compute_engines.compute_engine import ComputeEngine
from app.domain.compute_engines.handlers.otf_implementations.flox_otf_handler import (
    FloxOTFHandler,
)
from app.domain.compute_engines.handlers.precalc_implementations.precalc_handlers import (
    TreeCoverLossPrecalcHandler,
)
from app.domain.compute_engines.handlers.precalc_implementations.precalc_sql_query_builder import (
    PrecalcSqlQueryBuilder,
)
from app.domain.repositories.analysis_repository import AnalysisRepository
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
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn
from app.routers.land_change.tree_cover_loss.tree_cover_loss import (
    ANALYTICS_NAME,
    create_analysis_service,
    get_analysis_repository,
)
from app.use_cases.analysis.analysis_service import AnalysisService
from asgi_lifespan import LifespanManager
from fastapi import Depends, Request
from httpx import ASGITransport, AsyncClient


def get_file_system_analysis_repository() -> AnalysisRepository:
    return FileSystemAnalysisRepository(ANALYTICS_NAME)


def create_analysis_service_for_tests(
    request: Request,
    analysis_repository: AnalysisRepository = Depends(
        get_file_system_analysis_repository
    ),
) -> AnalysisService:
    compute_engine = ComputeEngine(
        handler=TreeCoverLossPrecalcHandler(
            precalc_query_builder=PrecalcSqlQueryBuilder(),
            precalc_query_service=DuckDbPrecalcQueryService(
                table_uri="s3://lcl-analytics/zonal_statistics/admin-tree-cover-loss-emissions-by-driver.parquet"
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
        analyzer=TreeCoverLossAnalyzer(compute_engine),
        event=ANALYTICS_NAME,
    )


class TestTclAnalyticsPostWithMultipleAdminAOIs:
    @pytest_asyncio.fixture
    async def setup(self):
        """Runs before each test in this class"""
        analytics_in = TreeCoverLossAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9", "IDN.14", "BRA"]),
            start_year="2015",
            end_year="2022",
            canopy_cover=30,
            intersections=[],
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
                request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json=analytics_in.model_dump(),
                )

                yield request, client, analytics_in

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
        assert "IDN.14" in df["aoi_id"].values
        assert "BRA" in df["aoi_id"].values

        assert ~(df.tree_cover_loss_year < 2015).any()
        assert ~(df.tree_cover_loss_year > 2022).any()

        assert df.columns.size == 5


class TestTclAnalyticsPostWithKba:
    @pytest_asyncio.fixture
    async def setup(self):
        """Runs before each test in this class"""
        analytics_in = TreeCoverLossAnalyticsIn(
            aoi=KeyBiodiversityAreaOfInterest(
                type="key_biodiversity_area", ids=["20401", "19426"]
            ),
            start_year="2020",
            end_year="2023",
            canopy_cover=30,
            intersections=[],
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
                request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json=analytics_in.model_dump(),
                )

                yield request, client, analytics_in

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
        assert "20401" in df["aoi_id"].values
        assert "19426" in df["aoi_id"].values

        assert ~(df.tree_cover_loss_year < 2020).any()
        assert ~(df.tree_cover_loss_year > 2023).any()

        assert df.columns.size == 5


class TestTclAnalyticsAdminAOIWithDriver:
    @pytest_asyncio.fixture
    async def setup(self):
        """Runs before each test in this class"""
        analytics_in = TreeCoverLossAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"]),
            start_year="2015",
            end_year="2022",
            canopy_cover=30,
            intersections=["driver"],
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
                request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json=analytics_in.model_dump(),
                )

                yield request, client, analytics_in

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

        assert "tree_cover_loss_driver" in df.columns
        assert "tree_cover_loss_year" not in df.columns
        assert df.columns.size == 5


class TestTclAnalyticsPostWithKbaWithDriver:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files("tree_cover_loss", "7d076b0a-fdde-58df-bbee-4da839fc1119")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    "/v0/land_change/tree_cover_loss/analytics",
                    json={
                        "aoi": {
                            "type": "key_biodiversity_area",
                            "ids": ["20401", "19426"],
                        },
                        "start_year": "2020",
                        "end_year": "2023",
                        "canopy_cover": 30,
                        "intersections": ["driver"],
                    },
                )

                yield (request, client)

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert resource["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/tree_cover_loss/analytics/7d076b0a-fdde-58df-bbee-4da839fc1119"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client = setup
        resource_id = test_request.json()["data"]["link"].split("/")[-1]
        data = await retry_getting_resource("tree_cover_loss", resource_id, client)

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert "20401" in df["aoi_id"].values
        assert "19426" in df["aoi_id"].values

        assert "tree_cover_loss_driver" in df.columns
        assert "tree_cover_loss_year" not in df.columns
        assert df.columns.size == 5


class TestTclAnalyticsWithForestFilters:
    @pytest_asyncio.fixture
    async def setup(self):
        """Runs before each test in this class"""
        analytics_in = TreeCoverLossAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["COL.1", "BRA.1"]),
            start_year="2020",
            end_year="2023",
            canopy_cover=30,
            forest_filter="primary_forest",
            intersections=[],
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
                request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json=analytics_in.model_dump(),
                )

                yield request, client, analytics_in

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
        assert "COL.1" in df["aoi_id"].values
        assert "BRA.1" in df["aoi_id"].values

        assert ~(df.tree_cover_loss_year < 2020).any()
        assert ~(df.tree_cover_loss_year > 2023).any()

        assert df.columns.size == 5
