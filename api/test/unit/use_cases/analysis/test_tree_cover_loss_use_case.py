import pytest

from uuid import UUID
from unittest.mock import MagicMock

from app.use_cases.analysis.tree_cover_loss.tree_cover_loss_service import TreeCoverLossService
from app.domain.analyzers.tree_cover_loss_analyzer import TreeCoverLossAnalyzer
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.domain.models.analysis import Analysis

from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn
from models.common.analysis import AnalysisStatus


class TestTreeCoverLossServiceCollaborators:
    @pytest.mark.asyncio
    async def test_happy_path_flow_with_no_status_does_do_analysis(self):
        stub_analysis_in = TreeCoverLossAnalyticsIn(
            aoi={
                "type": "protected_area",
                "ids": ["1234"],
            },
            start_year="2020",
            end_year="2021",
            canopy_cover=30,
            intersections=[]
        )

        mock_analysis_repository = MagicMock(spec=AnalysisRepository)
        mock_analysis_repository.load_analysis.side_effect = [
            Analysis(
                result=None,
                metadata=None,
                status=None
            )
        ]
        mock_analyzer = MagicMock(spec=TreeCoverLossAnalyzer)

        tree_cover_loss_service = TreeCoverLossService(
            analysis_repository=mock_analysis_repository,
            analyzer=mock_analyzer
        )

        await tree_cover_loss_service.set_resource_from(stub_analysis_in)
        result_thumbprint = tree_cover_loss_service.resource_thumbprint()
        await tree_cover_loss_service.do()

        mock_analysis_repository.load_analysis.assert_called_once_with(UUID('352564f7-37ec-5211-93b6-123b85b37295'))
        mock_analysis_repository.store_analysis.assert_called_once_with(
            UUID('352564f7-37ec-5211-93b6-123b85b37295'),
            Analysis(
                metadata=stub_analysis_in.model_dump(),
                result=None,
                status=AnalysisStatus.pending,
            )
        )
        assert result_thumbprint == UUID('352564f7-37ec-5211-93b6-123b85b37295')
        mock_analyzer.analyze.assert_called_once_with(
            Analysis(
                metadata=stub_analysis_in.model_dump(),
                result=None,
                status=AnalysisStatus.pending,
            )
        )

    @pytest.mark.asyncio
    async def test_happy_path_flow_with_pending_status_does_not_do_analysis(self):
        stub_analysis_in = TreeCoverLossAnalyticsIn(
            aoi={
                "type": "protected_area",
                "ids": ["1234"],
            },
            start_year="2020",
            end_year="2021",
            canopy_cover=30,
            intersections=[]
        )

        stub_domain_analysis = Analysis(
            result=None,
            metadata=stub_analysis_in.model_dump(),
            status=AnalysisStatus.pending
        )

        mock_analysis_repository = MagicMock(spec=AnalysisRepository)
        mock_analysis_repository.load_analysis.side_effect = [ stub_domain_analysis ]
        mock_analyzer = MagicMock(spec=TreeCoverLossAnalyzer)

        tree_cover_loss_service = TreeCoverLossService(
            analysis_repository=mock_analysis_repository,
            analyzer=mock_analyzer
        )

        await tree_cover_loss_service.set_resource_from(stub_analysis_in)
        result_thumbprint = tree_cover_loss_service.resource_thumbprint()
        await tree_cover_loss_service.do()

        mock_analysis_repository.load_analysis.assert_called_once_with(UUID('352564f7-37ec-5211-93b6-123b85b37295'))
        assert result_thumbprint == UUID('352564f7-37ec-5211-93b6-123b85b37295')
        mock_analysis_repository.store_analysis.assert_not_called()
        mock_analyzer.analyze.assert_not_called()


    @pytest.mark.asyncio
    async def test_happy_path_flow_with_saved_status_does_not_do_analysis(self):
        stub_analysis_in = TreeCoverLossAnalyticsIn(
            aoi={
                "type": "protected_area",
                "ids": ["1234"],
            },
            start_year="2020",
            end_year="2021",
            canopy_cover=30,
            intersections=[]
        )

        stub_domain_analysis = Analysis(
            result={ "fake": "result" },
            metadata=stub_analysis_in.model_dump(),
            status=AnalysisStatus.saved
        )

        mock_analysis_repository = MagicMock(spec=AnalysisRepository)
        mock_analysis_repository.load_analysis.side_effect = [ stub_domain_analysis ]
        mock_analyzer = MagicMock(spec=TreeCoverLossAnalyzer)

        tree_cover_loss_service = TreeCoverLossService(
            analysis_repository=mock_analysis_repository,
            analyzer=mock_analyzer
        )

        await tree_cover_loss_service.set_resource_from(stub_analysis_in)
        result_thumbprint = tree_cover_loss_service.resource_thumbprint()
        await tree_cover_loss_service.do()

        mock_analysis_repository.load_analysis.assert_called_once_with(UUID('352564f7-37ec-5211-93b6-123b85b37295'))
        assert result_thumbprint == UUID('352564f7-37ec-5211-93b6-123b85b37295')
        mock_analysis_repository.store_analysis.assert_not_called()
        mock_analyzer.analyze.assert_not_called()