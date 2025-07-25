import pytest
from unittest.mock import patch

from app.main import app
from fastapi.testclient import TestClient
from app.use_cases.analysis.tree_cover_loss.tree_cover_loss_service import (
    TreeCoverLossService,
)
from app.routers.land_change.tree_cover_loss.tree_cover_loss import (
    TreeCoverLossAnalyticsIn,
)

client = TestClient(app)


class TestTreeCoverLossPostUseCaseInitiation:
    @patch(
        "app.use_cases.analysis.tree_cover_loss.tree_cover_loss_service.TreeCoverLossService.do"
    )
    def test_dates_happy_path_start_year_is_before_end_year(
        self, tree_cover_loss_service: TreeCoverLossService
    ):
        tree_cover_loss_analytics_in = TreeCoverLossAnalyticsIn(
            aoi= {
            "type": "admin",
            "ids": ["IDN.24.9"],
            },
            start_year="2023",
            end_year="2024",
            canopy_cover=30,
            intersections=["natural_lands"],
        )
        client.post(
            "/v0/land_change/tree_cover_loss/analytics",
            json={
                "aoi": {"type": "admin", "ids": ["IDN.24.9"]},
                "start_year": "2023",
                "end_year": "2024",
                "canopy_cover": 30,
                "intersections": ["natural_lands"],
            },
        )

        tree_cover_loss_service.assert_called_with(tree_cover_loss_analytics_in)

    @pytest.mark.xfail
    def test_post_returns_pending_status(self):
        resource = self.test_request.json()
        assert resource["status"] == "pending"

    @pytest.mark.xfail
    def test_post_returns_resource_link(self):
        resource = self.test_request.json()
        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/tree_cover_loss/analytics/9c4b9bb5-0ecd-580d-85c9-d9a112a69b59"
        )

    @pytest.mark.xfail
    def test_post_202_accepted_response_code(self):
        response = self.test_request
        assert response.status_code == 202
