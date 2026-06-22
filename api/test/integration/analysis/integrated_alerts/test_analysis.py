from test.integration import delete_resource_files, retry_getting_resource

import pandas as pd
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import Depends, Request
from httpx import ASGITransport, AsyncClient

from app.domain.analyzers.integrated_alerts_analyzer import (
    INPUT_URIS,
    IntegratedAlertsAnalyzer,
)
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
    CustomAreaOfInterest,
)
from app.models.land_change.integrated_alerts import (
    ANALYTICS_NAME,
    IntegratedAlertsAnalyticsIn,
)
from app.routers.land_change.integrated_alerts.integrated_alerts import (
    create_analysis_service,
    get_analysis_repository,
)
from app.use_cases.analysis.analysis_service import AnalysisService, resource_thumbprint

# The precomputed parquet currently carries alert dates in the 2029-2032 range
# (a known pipeline epoch offset), so the integration window targets those dates.
START_DATE = "2029"
END_DATE = "2032"


def get_file_system_analysis_repository():
    return FileSystemAnalysisRepository(ANALYTICS_NAME)


def create_analysis_service_for_tests(
    request: Request, analysis_repository=Depends(get_file_system_analysis_repository)
) -> AnalysisService:
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=IntegratedAlertsAnalyzer(
            compute_engine=request.app.state.dask_client,
            duckdb_query_service=DuckDbPrecalcQueryService(
                table_uri=INPUT_URIS[Environment.production]["admin_results_table_uri"]
            ),
            input_uris=INPUT_URIS[Environment.production],
        ),
        event=ANALYTICS_NAME,
    )


async def post_analytics(analytics_in):
    analyzer = IntegratedAlertsAnalyzer(input_uris=INPUT_URIS[Environment.production])
    resource_tp = resource_thumbprint(analytics_in, analyzer)

    delete_resource_files(ANALYTICS_NAME, resource_tp)

    app.dependency_overrides[create_analysis_service] = (
        create_analysis_service_for_tests
    )
    app.dependency_overrides[get_analysis_repository] = (
        get_file_system_analysis_repository
    )

    async with LifespanManager(app):
        async with AsyncClient(
            transport=ASGITransport(app), base_url="http://testserver"
        ) as client:
            request = await client.post(
                f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                json=analytics_in.model_dump(),
            )
            yield request, client, resource_tp

    app.dependency_overrides.clear()


class TestIntegratedAlertsPostWithMultipleAdminAOIs:
    @pytest_asyncio.fixture
    async def setup(self):
        analytics_in = IntegratedAlertsAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9", "BRA.1"]),
            start_date=START_DATE,
            end_date=END_DATE,
        )
        async for result in post_analytics(analytics_in):
            yield result

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, setup):
        test_request, _, _ = setup
        assert test_request.json()["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, _, resource_tp = setup
        resource = test_request.json()
        expected_link = (
            f"http://testserver/v0/land_change/{ANALYTICS_NAME}"
            f"/analytics/{resource_tp}"
        )
        assert resource["data"]["link"] == expected_link

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
        assert "BRA.1" in df["aoi_id"].values
        assert df.area_ha.any()
        assert (df.aoi_type == "admin").all()
        # aoi_id, aoi_type, alert_date, alert_confidence, area_ha
        assert df.columns.size == 5


class TestIntegratedAlertsPostWithFeatureCollection:
    @pytest_asyncio.fixture
    async def setup(self):
        analytics_in = IntegratedAlertsAnalyticsIn(
            aoi=CustomAreaOfInterest(
                type="feature_collection",
                feature_collection={
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"id": "test_otf"},
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                    [
                                        [102.0, 0.0],
                                        [102.05, 0.0],
                                        [102.05, 0.05],
                                        [102.0, 0.05],
                                        [102.0, 0.0],
                                    ]
                                ],
                            },
                        }
                    ],
                },
            ),
            start_date=START_DATE,
            end_date=END_DATE,
        )
        async for result in post_analytics(analytics_in):
            yield result

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        _, client, resource_tp = setup
        data = await retry_getting_resource(ANALYTICS_NAME, resource_tp, client)

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert (df.aoi_id == "test_otf").all()
        assert (df.aoi_type == "feature").all()
        assert df.area_ha.any()
        assert set(df.alert_confidence.unique()).issubset({"low", "high", "highest"})
        # aoi_id, aoi_type, alert_date, alert_confidence, area_ha
        assert df.columns.size == 5
