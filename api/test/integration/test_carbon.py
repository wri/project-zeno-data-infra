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
            "c9923e65-4275-51c3-ba54-bce7c098f782",
        )

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                test_request = await client.post(
                    "/v0/land_change/carbon_flux/analytics",
                    json={
                        "aoi": {"type": "admin", "ids": ["NGA.20.31", "IDN.25.3"]},
                        "canopy_cover": 30,
                    },
                )

                yield test_request, client

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client = setup
        assert test_request.status_code == 202
        response = test_request.json()
        assert (
            response["data"]["link"]
            == "http://testserver/v0/land_change/carbon_flux/analytics/c9923e65-4275-51c3-ba54-bce7c098f782"
        )
        resource_id = response["data"]["link"].split("/")[-1]
        resource = await retry_getting_resource("carbon_flux", resource_id, client)

        expected = pd.DataFrame(
            {
                "aoi_id": ["NGA.20.31", "IDN.25.3"],
                "aoi_type": ["admin", "admin"],
                "carbon_net_flux_Mg_CO2e": [-12.349344253540039, 24370083.802587524],
                "carbon_gross_removals_Mg_CO2e": [
                    12.349344253540039,
                    28127167.23389721,
                ],
                "carbon_gross_emissions_Mg_CO2e": [None, 52405364.698329926],
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
            "f96d819d-b0e9-5314-a560-e72492e51df5",
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
                                        [97.600086293, 2.700319552],
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
                    json={"aoi": aoi, "canopy_cover": 50},
                )

                yield test_request, client

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client = setup
        assert test_request.status_code == 202
        response = test_request.json()
        assert (
            response["data"]["link"]
            == "http://testserver/v0/land_change/carbon_flux/analytics/f96d819d-b0e9-5314-a560-e72492e51df5"
        )
        resource_id = response["data"]["link"].split("/")[-1]
        resource = await retry_getting_resource("carbon_flux", resource_id, client)

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
