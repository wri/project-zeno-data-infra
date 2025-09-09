from test.integration import delete_resource_files, retry_getting_resource

import pandas as pd
import pytest
import pytest_asyncio
from app.main import app
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient


class TestAnalyticsPostWithMultipleAdminAOIs:
    @pytest_asyncio.fixture()
    async def setup(self):
        delete_resource_files("tree_cover", "25c2de46-7f3b-595a-a4ad-d448dd779b53")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    "/v0/land_change/tree_cover/analytics",
                    json={
                        "aoi": {
                            "type": "admin",
                            "ids": ["IDN.24.9", "BRA.14"],
                        },
                        "canopy_cover": 15,
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
            == "http://testserver/v0/land_change/tree_cover/analytics/25c2de46-7f3b-595a-a4ad-d448dd779b53"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client = setup
        resource_id = test_request.json()["data"]["link"].split("/")[-1]
        data = await retry_getting_resource("tree_cover", resource_id, client)

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert "IDN.24.9" in df["aoi_id"].values
        assert "BRA.14" in df["aoi_id"].values

        assert df.area_ha.any()
        assert (
            df.columns.size == 4
        )  # aoi_id, aoi_type, area_ha, carbon_emissions_MgCO2e


class TestTreeCoverAnalyticsPostWithKba:
    @pytest_asyncio.fixture()
    async def setup(self):
        delete_resource_files("tree_cover", "45eb41d8-ae8a-5be8-b8c5-b4ddc213e6b5")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    "/v0/land_change/tree_cover/analytics",
                    json={
                        "aoi": {
                            "type": "key_biodiversity_area",
                            "ids": ["20401", "19426"],
                        },
                        "canopy_cover": 15,
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
            == "http://testserver/v0/land_change/tree_cover/analytics/45eb41d8-ae8a-5be8-b8c5-b4ddc213e6b5"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client = setup
        resource_id = test_request.json()["data"]["link"].split("/")[-1]
        data = await retry_getting_resource("tree_cover", resource_id, client)

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert df.area_ha.any()
        assert (
            df.columns.size == 4
        )  # aoi_id, aoi_type, area_ha, carbon_emissions_MgCO2e
