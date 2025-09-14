import json
import os
from unittest.mock import MagicMock

import pytest
from app.main import app
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.common.base import DataMartResourceLink, DataMartResourceLinkResponse
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn
from app.routers.common_auth import verify_shared_secret
from app.routers.land_change.tree_cover.tree_cover import (
    create_analysis_service,
)
from app.use_cases.analysis.analysis_service import AnalysisService
from fastapi.testclient import TestClient

ENDPOINT_PATH = "/v0/land_change/tree_cover/analytics"
RESOURCE_THUMBPRINT = "a249c968-15d6-5777-9409-d29c63c63a6f"


@pytest.fixture
def dummy_analytics_in():
    return TreeCoverAnalyticsIn(
        aoi=AdminAreaOfInterest(
            type="admin",
            ids=["IDN.24.9"],
        ),
        canopy_cover=30,
    )


mock_service = MagicMock(spec=AnalysisService)


def create_mock_service():
    return mock_service


class TestTreeCoverPostUseCaseInitiation:
    @pytest.fixture
    def test_client(self):
        app.dependency_overrides[create_analysis_service] = create_mock_service
        app.dependency_overrides[verify_shared_secret] = lambda: None

        client = TestClient(app)

        try:
            yield client
        finally:
            app.dependency_overrides.clear()
            mock_service.reset_mock(return_value=True)

    def test_router_sets_the_resource_in_the_service(
        self, dummy_analytics_in, test_client
    ):
        mock_service.get_status.return_value = AnalysisStatus.pending

        _ = test_client.post(
            ENDPOINT_PATH,
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )
        mock_service.set_resource_from.assert_called_with(dummy_analytics_in)

    def test_router_returns_DataMartResourceLinkResponse(
        self, dummy_analytics_in, test_client
    ):
        mock_service.get_status.return_value = AnalysisStatus.pending
        mock_service.resource_thumbprint.return_value = RESOURCE_THUMBPRINT
        response = test_client.post(
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

    def test_post_202_accepted_response_code(self, dummy_analytics_in, test_client):
        mock_service.get_status.return_value = AnalysisStatus.pending

        response = test_client.post(
            ENDPOINT_PATH,
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )

        assert response.status_code == 202


class TestTreeCoverAuth:
    @pytest.fixture
    def test_client(self):
        app.dependency_overrides[create_analysis_service] = create_mock_service

        client = TestClient(app)

        yield client

        app.dependency_overrides.clear()
        mock_service.reset_mock(return_value=True)

    @pytest.mark.asyncio
    async def test_create_fails_on_bad_auth(self, dummy_analytics_in, test_client):
        response = test_client.post(
            ENDPOINT_PATH,
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_create_succeeds_on_good_auth(self, dummy_analytics_in, test_client):
        mock_service.get_status.return_value = AnalysisStatus.pending
        secret = os.getenv("ZENO_SECRET")

        response = test_client.post(
            ENDPOINT_PATH,
            headers={"X-Client-Secret": secret},
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )
        assert response.status_code == 202
