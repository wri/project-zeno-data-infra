import pytest
from app.main import app
from fastapi.testclient import TestClient

client = TestClient(app)


class TestTreeCover:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.test_request = client.post(
            "/v0/land_change/tree_cover/analytics",
            json={
                "aoi": {"type": "admin", "ids": ["IDN.24.9"]},
                "canopy_cover": 30,
                "forest_filter": "primary_forest",
            },
        )

    def test_post_returns_pending_status(self):
        resource = self.test_request.json()
        assert resource["status"] == "pending"

    def test_post_returns_resource_link(self):
        resource = self.test_request.json()
        assert (
            "http://testserver/v0/land_change/tree_cover/analytics"
            in resource["data"]["link"]
        )

    def test_post_202_accepted_response_code(self):
        response = self.test_request
        assert response.status_code == 202
