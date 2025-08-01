
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
            == "http://testserver/v0/land_change/grasslands/analytics/f1f3ddee-61c1-52aa-9c6a-ec16b3f14ac7"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, test_request):
        response = test_request
        assert response.status_code == 202


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
                            "aoi": aoi,
                            "start_year": "2003",
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
        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/grasslands/analytics/51a11cba-5d15-597d-864c-5b69f020cadd"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, test_request):
        response = test_request
        assert response.status_code == 202


class TestGrasslandsAnalyticsPostWithInvalidInput:
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
                            "aoi": {"type": "admin", "ids": "IDN.24.9"},
                            "start_year": "2000",
                            "end_year": "2023",
                        },
                    )

                    yield test_request


    @pytest.mark.asyncio
    async def test_post_with_invalid_input_returns_422(self, test_request):
        response = test_request
        assert response.status_code == 422