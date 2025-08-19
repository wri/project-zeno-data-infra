import json
from unittest.mock import MagicMock

import pytest
from app.main import app
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.common.base import DataMartResourceLink, DataMartResourceLinkResponse
from app.models.land_change.tree_cover_gain import TreeCoverGainAnalyticsIn
from app.routers.land_change.tree_cover_gain.tree_cover_gain import (
    create_analysis_service,
)
from app.use_cases.analysis.analysis_service import AnalysisService
from fastapi.testclient import TestClient

client = TestClient(app)

ENDPOINT_PATH = "/v0/land_change/tree_cover_gain/analytics"
RESOURCE_THUMBPRINT = "9b068e84-7b15-5ef2-b37d-415a7a844f4d"


@pytest.fixture
def dummy_analytics_in():
    return TreeCoverGainAnalyticsIn(
        aoi=AdminAreaOfInterest(
            type="admin",
            ids=["IDN.24.9"],
        ),
        start_year="2010",
        end_year="2015",
    )


mock_service = MagicMock(spec=AnalysisService)


def create_mock_service():
    return mock_service


class TestTreeCoverGainPostUseCaseInitiation:
    @pytest.mark.asyncio
    async def test_router_sets_the_resource_in_the_service(self, dummy_analytics_in):
        app.dependency_overrides[create_analysis_service] = create_mock_service
        mock_service.get_status.return_value = AnalysisStatus.pending

        client.post(
            ENDPOINT_PATH,
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )

        mock_service.set_resource_from.assert_called_with(dummy_analytics_in)

        mock_service.reset_mock(return_value=True)

    def test_router_returns_DataMartResourceLinkResponse(self, dummy_analytics_in):
        app.dependency_overrides[create_analysis_service] = create_mock_service

        mock_service.get_status.return_value = AnalysisStatus.pending
        mock_service.resource_thumbprint.return_value = RESOURCE_THUMBPRINT
        response = client.post(
            ENDPOINT_PATH,
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )

        mock_service.reset_mock(return_value=True)

        assert response.json() == json.loads(
            DataMartResourceLinkResponse(
                data=DataMartResourceLink(
                    link=f"http://testserver{ENDPOINT_PATH}/{RESOURCE_THUMBPRINT}"
                ),
                status=AnalysisStatus.pending,
            ).model_dump_json()
        )

    def test_post_202_accepted_response_code(self, dummy_analytics_in):
        app.dependency_overrides[create_analysis_service] = create_mock_service
        mock_service.get_status.return_value = AnalysisStatus.pending

        response = client.post(
            ENDPOINT_PATH,
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )

        mock_service.reset_mock(return_value=True)

        assert response.status_code == 202
