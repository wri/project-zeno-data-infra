import pytest
from app.main import app
from fastapi.testclient import TestClient

client = TestClient(app)

@pytest.mark.xfail
class TestTreeCoverLossPostWhenNoPreviousRequest:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        dir_path = delete_resource_files("9c4b9bb5-0ecd-580d-85c9-d9a112a69b59")
        write_metadata_file(dir_path)

        # now, the resource is already processing...make another post
        self.test_request = client.post(
            "/v0/land_change/dist_alerts/analytics",
            json={
                "aois": [{"type": "admin", "id": "IDN.24.9"}],
                "start_date": "2024-08-15",
                "end_date": "2024-08-16",
                "intersections": [],
            },
        )

    def test_post_returns_pending_status(self):
        resource = self.test_request.json()
        assert resource["status"] == "pending"

    def test_post_returns_resource_link(self):
        resource = self.test_request.json()
        assert (
                resource["data"]["link"]
                == "http://testserver/v0/land_change/dist_alerts/analytics/9c4b9bb5-0ecd-580d-85c9-d9a112a69b59"
        )

    def test_post_202_accepted_response_code(self):
        response = self.test_request
        assert response.status_code == 202