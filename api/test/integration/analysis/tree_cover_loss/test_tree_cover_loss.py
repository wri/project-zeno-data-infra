from test.integration import delete_resource_files, retry_getting_resource

import pandas as pd
import pytest
import pytest_asyncio
from app.main import app
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient


class TestTclAnalyticsPostWithMultipleAdminAOIs:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files("tree_cover_loss", "81cab9e3-9ba5-55af-8458-37422bac222c")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    "/v0/land_change/tree_cover_loss/analytics",
                    json={
                        "aoi": {
                            "type": "admin",
                            "ids": ["IDN.24.9", "IDN.14", "BRA"],
                        },
                        "start_year": "2015",
                        "end_year": "2022",
                        "canopy_cover": 30,
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
            == "http://testserver/v0/land_change/tree_cover_loss/analytics/81cab9e3-9ba5-55af-8458-37422bac222c"
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

        df = pd.DataFrame(data["result"])
        assert "IDN.24.9" in df["id"].values
        assert "IDN.14" in df["id"].values
        assert "BRA" in df["id"].values

        assert ~(df.loss_year < 2015).any()
        assert ~(df.loss_year > 2022).any()

        assert df.columns.size == 3
