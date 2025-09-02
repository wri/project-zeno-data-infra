from test.integration import (  # write_data_file,; write_metadata_file,
    delete_resource_files,
    retry_getting_resource,
)

import pandas as pd
import pytest
import pytest_asyncio
from app.main import app
from asgi_lifespan import LifespanManager
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

client = TestClient(app)


class TestCarbonDataAdmin:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files(
            "carbon_flux",
            "307dd9c9-9adb-581d-8113-bd3af71764e5",
        )

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                test_request = await client.post(
                    "/v0/land_change/carbon_flux/analytics",
                    json={"aoi": {"type": "admin", "ids": ["NGA.20.31", "IDN.25.3"]},
                          "canopy_cover": 30},
                )

                yield test_request, client

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/carbon_flux/analytics/307dd9c9-9adb-581d-8113-bd3af71764e5"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _ = setup
        response = test_request
        assert response.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client = setup
        resource_id = test_request.json()["data"]["link"].split("/")[-1]
        resource = await retry_getting_resource(
            "carbon_flux", resource_id, client
        )

        expected = pd.DataFrame(
            {
                "aoi_id": ["NGA.20.31", "IDN.25.3"],
                "aoi_type": ["admin", "admin"],
                "carbon_net_flux_Mg_CO2e": [-12.349344253540039, -31076.35616491735],
                "carbon_gross_removals_Mg_CO2e": [12.349344253540039, 2091621.1211385727],
                "carbon_gross_emissions_Mg_CO2e": [None, 2060545.7846540213],
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
        """Runs before each test in this class"""
        delete_resource_files(
            "carbon_flux",
            "456cdbe3-2b3c-5531-a425-afe8c3c395da",
        )

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                feature_collection = {
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
                                        [97.600086293, 2.700319552]
                                    ]
                                ],
                            },
                        }
                    ],
                }
                aoi = {
                    "type": "feature_collection",
                    "feature_collection": feature_collection,
                }
                test_request = await client.post(
                    "/v0/land_change/carbon_flux/analytics",
                    json={"aoi": aoi,
                          "canopy_cover": 50},
                )

                yield test_request, client

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/carbon_flux/analytics/456cdbe3-2b3c-5531-a425-afe8c3c395da"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _ = setup
        response = test_request
        assert response.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client = setup
        resource_id = test_request.json()["data"]["link"].split("/")[-1]
        resource = await retry_getting_resource(
            "carbon_flux", resource_id, client
        )

        expected = pd.DataFrame(
            {
                "aoi_id": ["test_aoi"],
                "aoi_type": ["feature"],
                "carbon_net_flux_Mg_CO2e": [322.671234],
                "carbon_gross_removals_Mg_CO2e": [732.29187],
                "carbon_gross_emissions_Mg_CO2e": [1054.9630126953125],
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
