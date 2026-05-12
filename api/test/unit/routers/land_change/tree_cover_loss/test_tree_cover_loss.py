import json
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.common.base import DataMartResourceLink, DataMartResourceLinkResponse
from app.routers.land_change.tree_cover_loss.tree_cover_loss import (
    TreeCoverLossAnalyticsIn,
    create_analysis_service,
)
from app.use_cases.analysis.analysis_service import AnalysisService

client = TestClient(app)


@pytest.fixture
def dummy_analytics_in():
    analytics_in = TreeCoverLossAnalyticsIn(
        aoi=AdminAreaOfInterest(
            type="admin",
            ids=["IDN.24.9"],
        ),
        start_year="2023",
        end_year="2024",
        canopy_cover=30,
        forest_filter="primary_forest",
        intersections=["driver"],
    )
    return analytics_in


mock_service = MagicMock(spec=AnalysisService)


def create_mock_service():
    return mock_service


class TestTreeCoverLossPostUseCaseInitiation:
    @pytest.mark.asyncio
    async def test_router_sets_the_resource_in_the_service(self, dummy_analytics_in):
        app.dependency_overrides[create_analysis_service] = create_mock_service
        mock_service.get_status.return_value = AnalysisStatus.pending

        client.post(
            "/v0/land_change/tree_cover_loss/analytics",
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )

        actual_arg = mock_service.set_resource_from.call_args[0][0]
        expected = dummy_analytics_in.model_dump()  # _environment still None here
        actual = actual_arg.model_dump()
        assert actual["aoi"] == expected["aoi"]
        assert actual["canopy_cover"] == expected["canopy_cover"]
        assert actual["forest_filter"] == expected["forest_filter"]
        assert actual["intersections"] == expected["intersections"]
        assert actual["start_year"] == expected["start_year"]
        assert actual["end_year"] == expected["end_year"]
        assert actual["_version"] == expected["_version"]
        assert actual["_analytics_name"] == expected["_analytics_name"]

        mock_service.reset_mock(return_value=True)

    def test_router_returns_DataMartResourceLinkResponse(self, dummy_analytics_in):
        app.dependency_overrides[create_analysis_service] = create_mock_service

        mock_service.get_status.return_value = AnalysisStatus.pending
        mock_service.resource_thumbprint.return_value = dummy_analytics_in.thumbprint()
        response = client.post(
            "/v0/land_change/tree_cover_loss/analytics",
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )

        mock_service.reset_mock(return_value=True)

        assert response.json() == json.loads(
            DataMartResourceLinkResponse(
                data=DataMartResourceLink(
                    link=f"http://testserver/v0/land_change/tree_cover_loss/analytics/{dummy_analytics_in.thumbprint()}"
                ),
                status=AnalysisStatus.pending,
            ).model_dump_json()
        )

    def test_post_202_accepted_response_code(self, dummy_analytics_in):
        app.dependency_overrides[create_analysis_service] = create_mock_service
        mock_service.get_status.return_value = AnalysisStatus.pending

        response = client.post(
            "/v0/land_change/tree_cover_loss/analytics",
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )

        mock_service.reset_mock(return_value=True)

        assert response.status_code == 202
