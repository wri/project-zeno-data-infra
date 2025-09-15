from unittest.mock import AsyncMock, MagicMock, call
from uuid import UUID

import pytest
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.models.common.analysis import AnalysisStatus, AnalyticsIn
from app.models.common.areas_of_interest import ProtectedAreaOfInterest
from app.use_cases.analysis.analysis_service import AnalysisService

resource_thumbprint = UUID("fe76241f-63d6-5e3f-8090-e9e8e98ea9aa")


@pytest.fixture
def stub_analysis_in():
    return AnalyticsIn(
        aoi=ProtectedAreaOfInterest(
            type="protected_area",
            ids=["1234"],
        )
    )


@pytest.fixture
def mock_analysis_repository():
    return MagicMock(spec=AnalysisRepository)


@pytest.fixture
def mock_analyzer():
    return AsyncMock()


class TestTreeCoverLossServiceCollaborators:
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

        analysis_service = AnalysisService(
            analysis_repository=mock_analysis_repository,
            analyzer=mock_analyzer,
            event="test_endpoint_name",
        )

        ############
        # Act      #
        ############
        await analysis_service.set_resource_from(stub_analysis_in)
        result_thumbprint = analysis_service.resource_thumbprint()
        await analysis_service.do()

        ############
        # Assert  #
        ############
        mock_analysis_repository.load_analysis.assert_called_once_with(
            resource_thumbprint
        )

        mock_analysis_repository_calls = (
            mock_analysis_repository.store_analysis.mock_calls
        )
        assert mock_analysis_repository.store_analysis.call_count == 3

        # assert that the new Analysis is stored so it's available to clients immediately
        assert mock_analysis_repository_calls[0] == call(
            stub_analysis_in.thumbprint(),
            Analysis(
                None,
                {"aoi": {}, "_version": "v0", "_analytics_name": "analytics"},
                None,
            ),
        )

        # assert that the new Analysis is stored as pending before analysis starts
        assert mock_analysis_repository_calls[1] == call(
            stub_analysis_in.thumbprint(),
            Analysis(
                None,
                {"aoi": {}, "_version": "v0", "_analytics_name": "analytics"},
                AnalysisStatus.pending,
            ),
        )

        # assert that the failed Analysis result is stored at the end
        assert mock_analysis_repository_calls[2] == call(
            stub_analysis_in.thumbprint(),
            Analysis(
                None,
                {"aoi": {}, "_version": "v0", "_analytics_name": "analytics"},
                AnalysisStatus.failed,
            ),
        )

        assert result_thumbprint == resource_thumbprint
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

        analysis_service = AnalysisService(
            analysis_repository=mock_analysis_repository,
            analyzer=mock_analyzer,
            event="test_endpoint_name",
        )

        ############
        # Act      #
        ############
        await analysis_service.set_resource_from(stub_analysis_in)
        result_thumbprint = analysis_service.resource_thumbprint()
        await analysis_service.do()

        ############
        # Assert  #
        ############
        mock_analysis_repository.load_analysis.assert_called_once_with(
            resource_thumbprint
        )
        assert result_thumbprint == resource_thumbprint
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
                result={"fake": "result"},
                metadata=stub_analysis_in.model_dump(),
                status=AnalysisStatus.saved,
            )
        ]

        analysis_service = AnalysisService(
            analysis_repository=mock_analysis_repository,
            analyzer=mock_analyzer,
            event="test_endpoint_name",
        )

        ############
        # Act      #
        ############
        await analysis_service.set_resource_from(stub_analysis_in)
        result_thumbprint = analysis_service.resource_thumbprint()
        await analysis_service.do()

        ############
        # Assert   #
        ############
        mock_analysis_repository.load_analysis.assert_called_once_with(
            resource_thumbprint
        )
        assert result_thumbprint == resource_thumbprint
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

        analysis_service = AnalysisService(
            analysis_repository=mock_analysis_repository,
            analyzer=mock_analyzer,
            event="test_endpoint_name",
        )

        ############
        # Act      #
        ############
        await analysis_service.set_resource_from(stub_analysis_in)
        result_thumbprint = analysis_service.resource_thumbprint()
        await analysis_service.do()

        ############
        # Assert   #
        ############
        mock_analysis_repository.load_analysis.assert_called_once_with(
            resource_thumbprint
        )
        assert result_thumbprint == resource_thumbprint
        mock_analysis_repository.store_analysis.assert_not_called()
        mock_analyzer.analyze.assert_not_called()

    @pytest.mark.asyncio
    async def test_analysis_exception_sets_status_to_failed(
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

        mock_analyzer.analyze.side_effect = [Exception("Test exception")]

        analysis_service = AnalysisService(
            analysis_repository=mock_analysis_repository,
            analyzer=mock_analyzer,
            event="test_endpoint_name",
        )

        ############
        # Act      #
        ############
        await analysis_service.set_resource_from(stub_analysis_in)
        result_thumbprint = analysis_service.resource_thumbprint()
        await analysis_service.do()

        ############
        # Assert   #
        ############
        mock_analysis_repository.load_analysis.assert_called_once_with(
            resource_thumbprint
        )
        assert result_thumbprint == resource_thumbprint
        mock_analyzer.analyze.assert_called()
        mock_analysis_repository.store_analysis.assert_called_with(
            stub_analysis_in.thumbprint(),
            Analysis(
                metadata={"aoi": {}, "_version": "v0", "_analytics_name": "analytics"},
                result=None,
                status=AnalysisStatus.failed,
            ),
        )
        assert analysis_service.get_status() == AnalysisStatus.failed
