from test.integration import (  # write_data_file,; write_metadata_file,
    delete_resource_files,
    retry_getting_resource,
)

import pandas as pd
import pytest
import pytest_asyncio
from app.domain.analyzers.carbon_flux_analyzer import CarbonFluxAnalyzer
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.main import app
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    CustomAreaOfInterest,
)
from app.models.land_change.carbon_flux import ANALYTICS_NAME, CarbonFluxAnalyticsIn
from app.routers.land_change.carbon_flux.carbon_flux import (
    create_analysis_service,
    get_analysis_repository,
)
from app.use_cases.analysis.analysis_service import AnalysisService
from asgi_lifespan import LifespanManager
from fastapi import Depends, Request
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

client = TestClient(app)


def get_file_system_analysis_repository():
    return FileSystemAnalysisRepository(ANALYTICS_NAME)


def create_analysis_service_for_tests(
    request: Request, analysis_repository=Depends(get_file_system_analysis_repository)
) -> AnalysisService:
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=CarbonFluxAnalyzer(
            analysis_repository=analysis_repository,
            compute_engine=request.app.state.dask_client,
        ),
        event=ANALYTICS_NAME,
    )


class TestCarbonDataAdmin:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        analytics_in = CarbonFluxAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["NGA.20.31"]),
            canopy_cover=30,
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
    async def test_resource_calculate_results(self, setup):
        test_request, client, analysis_params = setup
        assert test_request.status_code == 202
        response = test_request.json()
        assert (
            response["data"]["link"]
            == f"http://testserver/v0/land_change/{ANALYTICS_NAME}/analytics/{analysis_params.thumbprint()}"
        )
        resource = await retry_getting_resource(
            ANALYTICS_NAME, analysis_params.thumbprint(), client
        )

        expected = pd.DataFrame(
            {
                "aoi_id": ["NGA.20.31"],
                "aoi_type": ["admin"],
                "carbon_net_flux_Mg_CO2e": [
                    -12.349344253540039,
                    # 24370083.802587524,
                    # -11422581500.146715,
                ],
                "carbon_gross_removals_Mg_CO2e": [
                    12.349344253540039,
                    # 28127167.23389721,
                    # 17417177319.904095,
                ],
                "carbon_gross_emissions_Mg_CO2e": [
                    0.0,
                    # 52496919.891059,
                    # 5994595888.622183,
                ],
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


class TestCarbonDataFeature:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        analytics_in = CarbonFluxAnalyticsIn(
            aoi=CustomAreaOfInterest(
                type="feature_collection",
                feature_collection={
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"id": "test_aoi"},
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                    [
                                        [97.600086293, 2.700319552],
                                        [97.600772938, 2.676315412],
                                        [97.653731922, 2.677687091],
                                        [97.650298695, 2.7005120321],
                                        [97.600086293, 2.700319552],
                                    ]
                                ],
                            },
                        }
                    ],
                },
            ),
            canopy_cover=50,
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
                    "/v0/land_change/carbon_flux/analytics",
                    json=analytics_in.model_dump(),
                )

                yield test_request, client, analytics_in

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client, analysis_params = setup
        assert test_request.status_code == 202
        response = test_request.json()
        assert (
            response["data"]["link"]
            == f"http://testserver/v0/land_change/{ANALYTICS_NAME}/analytics/{analysis_params.thumbprint()}"
        )
        resource = await retry_getting_resource(
            ANALYTICS_NAME, analysis_params.thumbprint(), client
        )

        expected = pd.DataFrame(
            {
                "aoi_id": ["test_aoi"],
                "aoi_type": ["feature"],
                "carbon_net_flux_Mg_CO2e": [32997.52734375],
                "carbon_gross_removals_Mg_CO2e": [10810.841796875],
                "carbon_gross_emissions_Mg_CO2e": [43808.37109375],
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
