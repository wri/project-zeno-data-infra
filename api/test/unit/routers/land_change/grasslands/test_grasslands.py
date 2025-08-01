
import pytest
from unittest.mock import patch
import pytest_asyncio
from app.main import app
from asgi_lifespan import LifespanManager
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

client = TestClient(app)


class TestGrasslandsAnalyticsPostWithAdmin:
    @pytest_asyncio.fixture(autouse=True)
    async def test_request(self):
        """Runs before each test in this class"""

        with patch("app.routers.land_change.grasslands.grasslands.Path.exists", return_value=False) as mock_exists, \
            patch("app.routers.land_change.grasslands.grasslands.Path.mkdir") as mock_mkdir, \
            patch("app.routers.land_change.grasslands.grasslands.Path.write_text") as mock_write_text, \
            patch("app.routers.land_change.grasslands.grasslands.do_analytics") as mock_add_task:
            async with LifespanManager(app):
                async with AsyncClient(
                    transport=ASGITransport(app), base_url="http://testserver"
                ) as client:
                    test_request = await client.post(
                        "/v0/land_change/grasslands/analytics",
                        json={
                            "aoi": {"type": "admin", "ids": ["IDN.24.9"]},
                            "start_year": "2000",
                            "end_year": "2023",
                        },
                    )

                    yield test_request

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, test_request):
        resource = test_request.json()
        assert resource["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, test_request):

        resource = test_request.json()
        print("result", resource)
        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/grasslands/analytics/91d617f8-4d15-5b1c-9b49-3b742935356c"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, test_request):
        response = test_request
        assert response.status_code == 202


import pytest
from unittest.mock import patch
import pytest_asyncio
from app.main import app
from asgi_lifespan import LifespanManager
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

client = TestClient(app)


class TestGrasslandsAnalyticsPostWithCustomAOI:
    @pytest_asyncio.fixture(autouse=True)
    async def test_request(self):
        """Runs before each test in this class"""

        with patch("app.routers.land_change.grasslands.grasslands.Path.exists", return_value=False) as mock_exists, \
            patch("app.routers.land_change.grasslands.grasslands.Path.mkdir") as mock_mkdir, \
            patch("app.routers.land_change.grasslands.grasslands.Path.write_text") as mock_write_text, \
            patch("app.routers.land_change.grasslands.grasslands.do_analytics") as mock_add_task:
            async with LifespanManager(app):
                async with AsyncClient(
                    transport=ASGITransport(app), base_url="http://testserver"
                ) as client:
                    geojson = {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [105.00050, 47.99875],
                                [105.00175, 47.99875],
                                [105.00175, 47.99775],
                                [105.00050, 47.99775],
                                [105.00050, 47.99875],
                            ]
                        ],
                    }
                    aoi = {"type": "feature_collection", "feature_collection": geojson}
                    test_request = await client.post(
                        "/v0/land_change/grasslands/analytics",
                        json={
                            "aoi": aoi
                        },
                    )

                    yield test_request

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, test_request):
        resource = test_request.json()
        assert resource["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, test_request):

        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/grasslands/analytics/9067221f-fc65-56c8-a4c8-6af182b03240"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, test_request):
        response = test_request
        assert response.status_code == 202

