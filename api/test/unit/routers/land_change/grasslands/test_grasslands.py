import json
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.common.base import DataMartResourceLink, DataMartResourceLinkResponse
from app.models.land_change.grasslands import GrasslandsAnalyticsIn
from app.routers.land_change.grasslands.grasslands import create_analysis_service
from app.use_cases.analysis.analysis_service import AnalysisService

client = TestClient(app)

ENDPOINT_PATH = "/v0/land_change/grasslands/analytics"

mock_service = MagicMock(spec=AnalysisService)


def create_mock_service():
    return mock_service


@pytest.fixture
def dummy_analytics_in():
    return GrasslandsAnalyticsIn(
        aoi=AdminAreaOfInterest(
            type="admin",
            ids=["BRA.1.12"],
        ),
        start_year="2020",
        end_year="2022",
    )


class TestGrasslandsPostUseCaseInitiation:
    @pytest.mark.asyncio
    async def test_router_sets_the_resource_in_the_service(self, dummy_analytics_in):
        app.dependency_overrides[create_analysis_service] = create_mock_service
        mock_service.get_status.return_value = AnalysisStatus.pending

        client.post(
            ENDPOINT_PATH,
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )

        actual_arg = mock_service.set_resource_from.call_args[0][0]
        expected = dummy_analytics_in.model_dump()
        actual = actual_arg.model_dump()
        assert actual["aoi"] == expected["aoi"]
        assert actual["start_year"] == expected["start_year"]
        assert actual["end_year"] == expected["end_year"]
        assert actual["_version"] == expected["_version"]
        assert actual["_analytics_name"] == expected["_analytics_name"]

        mock_service.reset_mock(return_value=True)

    def test_router_returns_DataMartResourceLinkResponse(self, dummy_analytics_in):
        dummy_uuid = UUID("3ad81792-5086-4fa8-b4b1-b33afa91b990")
        app.dependency_overrides[create_analysis_service] = create_mock_service
        mock_service.get_status.return_value = AnalysisStatus.pending
        mock_service.resource_thumbprint.return_value = dummy_uuid

        response = client.post(
            ENDPOINT_PATH,
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )

        mock_service.reset_mock(return_value=True)

        assert response.json() == json.loads(
            DataMartResourceLinkResponse(
                data=DataMartResourceLink(
                    link=f"http://testserver{ENDPOINT_PATH}/{dummy_uuid}"
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
