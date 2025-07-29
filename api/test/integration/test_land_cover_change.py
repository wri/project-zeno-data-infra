import pandas as pd
import pytest
import pytest_asyncio
from app.main import app
from asgi_lifespan import LifespanManager
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

from api.test.integration import (  # write_data_file,; write_metadata_file,
    delete_resource_files,
    retry_getting_resource,
)

client = TestClient(app)


class TestLandCoverChangeMockData:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files("71f40812-2157-5ce2-b654-377e833e5f73")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                test_request = await client.post(
                    "/v0/land_change/land_cover_change/analytics",
                    json={"aoi": [{"type": "admin", "ids": ["BRA.1.12", "IDN.24.9"]}]},
                )

                yield test_request, client

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
            == "http://testserver/v0/land_change/land_cover_change/analytics/71f40812-2157-5ce2-b654-377e833e5f73"
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
        data = await retry_getting_resource(resource_id, client)

        expected_df = pd.DataFrame(
            {
                "id": (["BRA.1.12"] * 9) + (["IDN.24.8"] * 9),
                "land_cover_class_start": [
                    "Bare and sparse vegetation",
                    "Short vegetation",
                    "Tree cover",
                    "Wetland-short vegetation",
                    "Water",
                    "Snow/ice",
                    "Cropland",
                    "Built-up",
                    "Cultivated grasslands",
                ]
                * 2,
                "land_cover_class_end": [
                    "Cultivated grasslands",
                    "Built-up",
                    "Cropland",
                    "Snow/ice",
                    "Water",
                    "Wetland-short vegetation",
                    "Tree cover",
                    "Short vegetation",
                    "Bare and sparse vegetation",
                ]
                * 2,
                "area_ha": [1, 2, 3, 4, 5, 6, 7, 8, 9] * 2,
            }
        )

        actual_df = pd.DataFrame(data["result"])
        print(actual_df)

        pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)
