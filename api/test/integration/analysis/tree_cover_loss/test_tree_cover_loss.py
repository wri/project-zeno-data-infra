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
        delete_resource_files("tree_cover_loss", "17d38735-9e8b-5242-b1b2-31534835fd11")

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
                        "intersections": [],
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
            == "http://testserver/v0/land_change/tree_cover_loss/analytics/17d38735-9e8b-5242-b1b2-31534835fd11"
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

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert "IDN.24.9" in df["aoi_id"].values
        assert "IDN.14" in df["aoi_id"].values
        assert "BRA" in df["aoi_id"].values

        assert ~(df.tree_cover_loss_year < 2015).any()
        assert ~(df.tree_cover_loss_year > 2022).any()

        assert df.columns.size == 5


class TestTclAnalyticsPostWithKba:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files("tree_cover_loss", "fe77ddb5-0c77-5a5f-a825-7383d90d5710")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    "/v0/land_change/tree_cover_loss/analytics",
                    json={
                        "aoi": {
                            "type": "key_biodiversity_area",
                            "ids": ["20401", "19426"],
                        },
                        "start_year": "2020",
                        "end_year": "2023",
                        "canopy_cover": 30,
                        "intersections": [],
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
            == "http://testserver/v0/land_change/tree_cover_loss/analytics/fe77ddb5-0c77-5a5f-a825-7383d90d5710"
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

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert "20401" in df["aoi_id"].values
        assert "19426" in df["aoi_id"].values

        assert ~(df.tree_cover_loss_year < 2020).any()
        assert ~(df.tree_cover_loss_year > 2023).any()

        assert df.columns.size == 5


class TestTclAnalyticsAdminAOIWithDriver:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files("tree_cover_loss", "cb7cc597-2579-5ac0-9dc6-a2533a4576ad")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    "/v0/land_change/tree_cover_loss/analytics",
                    json={
                        "aoi": {
                            "type": "admin",
                            "ids": ["IDN.24.9"],
                        },
                        "start_year": "2015",
                        "end_year": "2022",
                        "canopy_cover": 30,
                        "intersections": ["driver"],
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
            == "http://testserver/v0/land_change/tree_cover_loss/analytics/cb7cc597-2579-5ac0-9dc6-a2533a4576ad"
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

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert "IDN.24.9" in df["aoi_id"].values

        assert "tree_cover_loss_driver" in df.columns
        assert "tree_cover_loss_year" not in df.columns
        assert df.columns.size == 5


class TestTclAnalyticsPostWithKbaWithDriver:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files("tree_cover_loss", "7d076b0a-fdde-58df-bbee-4da839fc1119")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    "/v0/land_change/tree_cover_loss/analytics",
                    json={
                        "aoi": {
                            "type": "key_biodiversity_area",
                            "ids": ["20401", "19426"],
                        },
                        "start_year": "2020",
                        "end_year": "2023",
                        "canopy_cover": 30,
                        "intersections": ["driver"],
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
            == "http://testserver/v0/land_change/tree_cover_loss/analytics/7d076b0a-fdde-58df-bbee-4da839fc1119"
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

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert "20401" in df["aoi_id"].values
        assert "19426" in df["aoi_id"].values

        assert "tree_cover_loss_driver" in df.columns
        assert "tree_cover_loss_year" not in df.columns
        assert df.columns.size == 5


class TestTclAnalyticsWithForestFilters:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files("tree_cover_loss", "0cd87715-e7eb-5b33-b6da-7d0b5143e625")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    "/v0/land_change/tree_cover_loss/analytics",
                    json={
                        "aoi": {
                            "type": "admin",
                            "ids": ["COL.1", "BRA.1"],
                        },
                        "start_year": "2020",
                        "end_year": "2023",
                        "canopy_cover": 30,
                        "forest_filter": "primary_forest",
                        "intersections": [],
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
            == "http://testserver/v0/land_change/tree_cover_loss/analytics/0cd87715-e7eb-5b33-b6da-7d0b5143e625"
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

        assert data["status"] == "saved"

        df = pd.DataFrame(data["result"])
        assert "COL.1" in df["aoi_id"].values
        assert "BRA.1" in df["aoi_id"].values

        assert ~(df.tree_cover_loss_year < 2020).any()
        assert ~(df.tree_cover_loss_year > 2023).any()

        assert df.columns.size == 5
