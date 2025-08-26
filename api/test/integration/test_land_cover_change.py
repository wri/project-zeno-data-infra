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


class TestLandCoverChangeData:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files(
            "land_cover_change",
            "4b5102d9-2eb7-58b1-8378-9beb8f180cea",
        )

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                test_request = await client.post(
                    "/v0/land_change/land_cover_change/analytics",
                    json={"aoi": {"type": "admin", "ids": ["NGA.20.31"]}},
                )

                yield test_request, client

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/land_cover_change/analytics/4b5102d9-2eb7-58b1-8378-9beb8f180cea"
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
            "land_cover_change", resource_id, client
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
            rtol=1e-4,
            atol=1e-4,
        )
