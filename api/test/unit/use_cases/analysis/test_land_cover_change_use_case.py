import pytest

from uuid import UUID
from unittest.mock import MagicMock

from app.use_cases.analysis.land_cover.land_cover_change import LandCoverChangeService
from app.domain.analyzers.land_cover_change_analyzer import LandCoverChangeAnalyzer
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.domain.models.analysis import Analysis

from app.models.land_change.land_cover import LandCoverChangeAnalyticsIn
from app.models.common.analysis import AnalysisStatus


@pytest.fixture
def stub_analysis_in():
    return LandCoverChangeAnalyticsIn(
        aoi={
            "type": "protected_area",
            "ids": ["1234"],
        }
    )


@pytest.fixture
def mock_analysis_repository():
    return MagicMock(spec=AnalysisRepository)


@pytest.fixture
def mock_analyzer():
    return MagicMock(spec=LandCoverChangeAnalyzer)


class TestLandCoverChangeServiceCollaborators:
    @pytest.mark.asyncio
    async def test_happy_path_flow_with_no_status_does_do_analysis(
        self,
        stub_analysis_in,
        mock_analysis_repository,
        mock_analyzer,
    ):
        ############
        # Arrange  #
        ############
        mock_analysis_repository.load_analysis.side_effect = [
            Analysis(result=None, metadata=None, status=None)
        ]

        land_cover_change_service = LandCoverChangeService(
            analysis_repository=mock_analysis_repository, analyzer=mock_analyzer
        )

        ############
        # Act      #
        ############
        await land_cover_change_service.set_resource_from(stub_analysis_in)
        result_thumbprint = land_cover_change_service.resource_thumbprint()
        await land_cover_change_service.do()

        ############
        # Assert  #
        ############
        mock_analysis_repository.load_analysis.assert_called_once_with(
            UUID("e60501fd-322e-5baa-b02d-bae90db39092")
        )

        mock_analysis_repository.store_analysis.assert_called_once_with(
            UUID("e60501fd-322e-5baa-b02d-bae90db39092"),
            Analysis(
                metadata=stub_analysis_in.model_dump(),
                result=None,
                status=AnalysisStatus.pending,
            ),
        )
        assert result_thumbprint == UUID("e60501fd-322e-5baa-b02d-bae90db39092")
        mock_analyzer.analyze.assert_called_once_with(
            Analysis(
                metadata=stub_analysis_in.model_dump(),
                result=None,
                status=AnalysisStatus.pending,
            )
        )

    @pytest.mark.asyncio
    async def test_happy_path_flow_with_pending_status_does_not_do_analysis(
        self,
        stub_analysis_in,
        mock_analysis_repository,
        mock_analyzer,
    ):
        ############
        # Arrange  #
        ############
        mock_analysis_repository.load_analysis.side_effect = [
            Analysis(
                result=None,
                metadata=stub_analysis_in.model_dump(),
                status=AnalysisStatus.pending,
            )
        ]

        land_cover_change_service = LandCoverChangeService(
            analysis_repository=mock_analysis_repository, analyzer=mock_analyzer
        )

        ############
        # Act      #
        ############
        await land_cover_change_service.set_resource_from(stub_analysis_in)
        result_thumbprint = land_cover_change_service.resource_thumbprint()
        await land_cover_change_service.do()

        ############
        # Assert  #
        ############
        mock_analysis_repository.load_analysis.assert_called_once_with(
            UUID("e60501fd-322e-5baa-b02d-bae90db39092")
        )
        assert result_thumbprint == UUID("e60501fd-322e-5baa-b02d-bae90db39092")
        mock_analysis_repository.store_analysis.assert_not_called()
        mock_analyzer.analyze.assert_not_called()

    @pytest.mark.asyncio
    async def test_happy_path_flow_with_saved_status_does_not_do_analysis(
        self,
        stub_analysis_in,
        mock_analysis_repository,
        mock_analyzer,
    ):
        ############
        # Arrange  #
        ############
        mock_analysis_repository.load_analysis.side_effect = [
            Analysis(
                result={
                    "change_area": [1000, 2000],
                    "aoi_id": ["1234", "1234"],
                    "aoi_type": ["protected_area", "protected_area"],
                    "land_cover_class_start": ["Tree cover", "Cultivated grasslands"],
                    "land_cover_class_end": ["Build-up", "Short vegetation"],
                },
                metadata=stub_analysis_in.model_dump(),
                status=AnalysisStatus.saved,
            )
        ]

        land_cover_change_service = LandCoverChangeService(
            analysis_repository=mock_analysis_repository, analyzer=mock_analyzer
        )

        ############
        # Act      #
        ############
        await land_cover_change_service.set_resource_from(stub_analysis_in)
        result_thumbprint = land_cover_change_service.resource_thumbprint()
        await land_cover_change_service.do()

        ############
        # Assert   #
        ############
        mock_analysis_repository.load_analysis.assert_called_once_with(
            UUID("e60501fd-322e-5baa-b02d-bae90db39092")
        )
        assert result_thumbprint == UUID("e60501fd-322e-5baa-b02d-bae90db39092")
        mock_analysis_repository.store_analysis.assert_not_called()
        mock_analyzer.analyze.assert_not_called()

    @pytest.mark.asyncio
    async def test_happy_path_flow_with_failed_status_does_not_do_analysis(
        self,
        stub_analysis_in,
        mock_analysis_repository,
        mock_analyzer,
    ):
        ############
        # Arrange  #
        ############
        mock_analysis_repository.load_analysis.side_effect = [
            Analysis(
                result=None,
                metadata=stub_analysis_in.model_dump(),
                status=AnalysisStatus.failed,
            )
        ]

        land_cover_change_service = LandCoverChangeService(
            analysis_repository=mock_analysis_repository, analyzer=mock_analyzer
        )

        ############
        # Act      #
        ############
        await land_cover_change_service.set_resource_from(stub_analysis_in)
        result_thumbprint = land_cover_change_service.resource_thumbprint()
        await land_cover_change_service.do()

        ############
        # Assert   #
        ############
        mock_analysis_repository.load_analysis.assert_called_once_with(
            UUID("e60501fd-322e-5baa-b02d-bae90db39092")
        )
        assert result_thumbprint == UUID("e60501fd-322e-5baa-b02d-bae90db39092")
        mock_analysis_repository.store_analysis.assert_not_called()
        mock_analyzer.analyze.assert_not_called()
