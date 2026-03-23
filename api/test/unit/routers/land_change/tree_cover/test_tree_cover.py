import json
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.domain.models.environment import Environment
from app.main import app
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.common.base import DataMartResourceLink, DataMartResourceLinkResponse
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn
from app.routers.land_change.tree_cover.tree_cover import (
    create_analysis_service,
)
from app.use_cases.analysis.analysis_service import AnalysisService

client = TestClient(app)

ENDPOINT_PATH = "/v0/land_change/tree_cover/analytics"


@pytest.fixture
def dummy_analytics_in():
    analytics_in = TreeCoverAnalyticsIn(
        aoi=AdminAreaOfInterest(
            type="admin",
            ids=["IDN.24.9"],
        ),
        canopy_cover=30,
    )
    analytics_in.set_input_uris(Environment.production)
    return analytics_in


mock_service = MagicMock(spec=AnalysisService)


def create_mock_service():
    return mock_service


class TestTreeCoverPostUseCaseInitiation:
    @pytest.mark.asyncio
    async def test_router_sets_the_resource_in_the_service(self, dummy_analytics_in):
        app.dependency_overrides[create_analysis_service] = create_mock_service
        mock_service.get_status.return_value = AnalysisStatus.pending

        client.post(
            ENDPOINT_PATH,
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )

        actual_arg = mock_service.set_resource_from.call_args[0][0]
        expected = dummy_analytics_in.model_dump()  # _environment still None here
        actual = actual_arg.model_dump()
        assert actual["aoi"] == expected["aoi"]
        assert actual["canopy_cover"] == expected["canopy_cover"]
        assert actual["forest_filter"] == expected["forest_filter"]
        assert actual["_version"] == expected["_version"]
        assert actual["_analytics_name"] == expected["_analytics_name"]
        # deliberately skip _environment — that's an infrastructure concern, not what this test is about

        mock_service.reset_mock(return_value=True)

    def test_router_returns_DataMartResourceLinkResponse(self, dummy_analytics_in):
        app.dependency_overrides[create_analysis_service] = create_mock_service

        mock_service.get_status.return_value = AnalysisStatus.pending
        mock_service.resource_thumbprint.return_value = dummy_analytics_in.thumbprint()
        response = client.post(
            ENDPOINT_PATH,
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )

        mock_service.reset_mock(return_value=True)

        assert response.json() == json.loads(
            DataMartResourceLinkResponse(
                data=DataMartResourceLink(
                    link=f"http://testserver{ENDPOINT_PATH}/{dummy_analytics_in.thumbprint()}"
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
