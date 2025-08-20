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


pytest.mark.xfail(reason="this was for the old implementation, needs to be updated.")
class TestLandCoverChangeMockData:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files(
            "land_cover_change",
            "a8df3000-5cf6-5050-8717-592310672f0d",
        )

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                test_request = await client.post(
                    "/v0/land_change/land_cover_change/analytics",
                    json={"aoi": {"type": "admin", "ids": ["BRA.1.12", "IDN.24.9"]}},
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
            == "http://testserver/v0/land_change/land_cover_change/analytics/a8df3000-5cf6-5050-8717-592310672f0d"
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

        assert (
            "BRA.1.12" in resource["result"]["aoi_id"]
        ), "Expected result to contain AOI IDs."
        assert (
            "IDN.24.9" in resource["result"]["aoi_id"]
        ), "Expected result to contain AOI IDs."

        df = pd.DataFrame(resource["result"])
        assert df.columns.tolist() == [
            "aoi_id",
            "aoi_type",
            "land_cover_class_start",
            "land_cover_class_end",
            "change_area",
        ], "Expected re6sult to have specific columns."
