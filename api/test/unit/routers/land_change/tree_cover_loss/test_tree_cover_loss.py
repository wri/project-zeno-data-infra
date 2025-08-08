import json
from unittest.mock import patch, MagicMock

import pytest
from app.main import app
from app.routers.land_change.tree_cover_loss.tree_cover_loss import TreeCoverLossAnalyticsIn
from app.use_cases.analysis.tree_cover_loss.tree_cover_loss_service import TreeCoverLossService
from fastapi.testclient import TestClient
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.common.base import DataMartResourceLinkResponse, DataMartResourceLink
from app.models.common.analysis import AnalysisStatus
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.domain.analyzers.tree_cover_loss_analyzer import TreeCoverLossAnalyzer

client = TestClient(app)

@pytest.fixture
def dummy_analytics_in():
    return TreeCoverLossAnalyticsIn(
        aoi=AdminAreaOfInterest(
            type= "admin",
            ids= ["IDN.24.9"],
        ),
        start_year="2023",
        end_year="2024",
        canopy_cover=30,
        forest_filter="primary_forest",
        intersections=["driver"],
    )

@pytest.fixture
def mock_analysis_repository():
    return MagicMock(spec=AnalysisRepository)

@pytest.fixture
def mock_analyzer():
    return MagicMock(spec=TreeCoverLossAnalyzer)


class TestTreeCoverLossPostUseCaseInitiation:
    def test_router_sets_the_resource_in_the_service(
        self,
        dummy_analytics_in,
        mock_analysis_repository,
        mock_analyzer
    ):
        app.dependency_overrides['get_analysis_repository'] = mock_analysis_repository
        app.dependency_overrides['get_analyzer'] = mock_analyzer
        with patch.object(TreeCoverLossService, 'set_resource_from') as mock_set_resource:
            client.post(
                "/v0/land_change/tree_cover_loss/analytics",
                json=json.loads(dummy_analytics_in.model_dump_json()),
            )

            mock_set_resource.assert_called_with(dummy_analytics_in)

    def test_router_returns_DataMartResourceLinkResponse(self, dummy_analytics_in):
        app.dependency_overrides['get_analysis_repository'] = mock_analysis_repository
        app.dependency_overrides['get_analyzer'] = mock_analyzer
        response = client.post(
            "/v0/land_change/tree_cover_loss/analytics",
            json=json.loads(dummy_analytics_in.model_dump_json()),
        )

        assert response.json() == json.loads(DataMartResourceLinkResponse(
            data=DataMartResourceLink(link="http://testserver/v0/land_change/tree_cover_loss/analytics/12665e7b-e976-5ab2-adc2-c4576399f0bb"),
            status=AnalysisStatus.pending
        ).model_dump_json())


    def test_post_202_accepted_response_code(self, dummy_analytics_in):
        app.dependency_overrides['get_analysis_repository'] = mock_analysis_repository
        app.dependency_overrides['get_analyzer'] = mock_analyzer
        with patch.object(TreeCoverLossService, 'do'):
            response = client.post(
                "/v0/land_change/tree_cover_loss/analytics",
                json=json.loads(dummy_analytics_in.model_dump_json()),
            )

            assert response.status_code == 202
