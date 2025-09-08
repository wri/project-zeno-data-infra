from test.integration import delete_resource_files, retry_getting_resource

import pandas as pd
import pytest
import pytest_asyncio
from app.main import app
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient


class TestGainAnalyticsPostWithMultipleAdminAOIs:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files("tree_cover_gain", "d0ec8847-c0f9-5429-84a9-08afd84cc58f")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    "/v0/land_change/tree_cover_gain/analytics",
                    json={
                        "aoi": {
                            "type": "admin",
                            "ids": ["IDN.24.9", "IDN.14", "BRA"],
                        },
                        "start_year": "2005",
                        "end_year": "2020",
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
            == "http://testserver/v0/land_change/tree_cover_gain/analytics/d0ec8847-c0f9-5429-84a9-08afd84cc58f"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client = setup
        resource_id = test_request.json()["data"]["link"].split("/")[-1]
        data = await retry_getting_resource("tree_cover_gain", resource_id, client)

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert "IDN.24.9" in df["aoi_id"].values
        assert "IDN.14" in df["aoi_id"].values
        assert "BRA" in df["aoi_id"].values

        assert ~(df.tree_cover_gain_period == "2000-2005").any()
        assert df.columns.size == 4


class TestGainAnalyticsPostWithKba:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files("tree_cover_gain", "0f48b87f-306c-5f6f-852c-ba92ae76c441")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    "/v0/land_change/tree_cover_gain/analytics",
                    json={
                        "aoi": {
                            "type": "key_biodiversity_area",
                            "ids": ["20401", "19426"],
                        },
                        "start_year": "2005",
                        "end_year": "2020",
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
            == "http://testserver/v0/land_change/tree_cover_gain/analytics/0f48b87f-306c-5f6f-852c-ba92ae76c441"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client = setup
        resource_id = test_request.json()["data"]["link"].split("/")[-1]
        data = await retry_getting_resource("tree_cover_gain", resource_id, client)

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert "20401" in df["aoi_id"].values
        assert "19426" in df["aoi_id"].values

        assert ~(df.tree_cover_gain_period == "2000-2005").any()
        assert df.columns.size == 4
