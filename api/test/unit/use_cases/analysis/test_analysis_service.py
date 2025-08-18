from unittest.mock import MagicMock
from uuid import UUID

import pytest
from app.domain.analyzers.tree_cover_loss_analyzer import DataAPIAnalyzer
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.models.common.analysis import AnalysisStatus, AnalyticsIn
from app.models.common.areas_of_interest import ProtectedAreaOfInterest
from app.use_cases.analysis.analysis_service import AnalysisService

resource_thumbprint = UUID("8eeb284d-cfe6-5f45-a353-e8eaf8bb302b")


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
    return MagicMock(spec=DataAPIAnalyzer)


class TestTreeCoverLossServiceCollaborators:
    @pytest.mark.asyncio
    async def test_happy_path_flow_with_no_status_does_do_analysis(
        self,
        stub_analysis_in,
        mock_analysis_repository,
        mock_analyzer,
    ):
        """
        As a climate analyst requesting new tree cover loss data
        When I submit analysis parameters for a protected area (2020-2021, 30% canopy)
        And the system has no prior record of this analysis
        Then the service should:

        1. Create a new analysis tracking ticket (UUID thumbprint)
        2. Reserve a pending slot in the repository
        3. Dispatch the analysis request to the processing engine
        4. Preserve my exact input parameters throughout the journey

        This is the sunshine scenario where:
        - The analysis request flows through all key collaborators
        - The system demonstrates its core orchestration behavior
        - Each component plays its designated role in the workflow
        """
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
        mock_analysis_repository.store_analysis.assert_called_once_with(
            resource_thumbprint,
            Analysis(
                metadata=stub_analysis_in.model_dump(),
                result=None,
                status=AnalysisStatus.pending,
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
        """
        As a climate analyst checking on my tree cover loss request
        When I submit identical parameters for a protected area (2020-2021, 30% canopy)
        And the system already has this analysis queued for processing
        Then the service should:

        1. Recognize the duplicate request using its unique fingerprint
        2. Preserve existing computational resources
        3. Avoid creating duplicate analysis jobs
        4. Maintain the original pending status unchanged

        This is the efficiency scenario where:
        - The system demonstrates intelligent request deduplication
        - We prevent wasteful recomputation of identical analyses
        - The service honors its 'exactly-once' processing guarantee
        """
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
        """
        As a climate analyst revisiting previous tree cover loss data
        When I request results for a protected area (2020-2021, 30% canopy)
        And the system recognizes this analysis was already completed
        Then the service should:

        1. Immediately retrieve the preserved results
        2. Bypass any unnecessary reprocessing
        3. Honor previous computational investments
        4. Deliver instant access to existing findings

        This is the result-reuse scenario where:
        - The system demonstrates its memory and efficiency
        - Users benefit from rapid response times
        - We avoid redundant environmental computations
        - Historical analyses remain perpetually accessible
        """
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
        """
        As a climate analyst checking on a problematic tree cover request
        When I resubmit parameters for a protected area (2020-2021, 30% canopy)
        And the system recognizes this analysis previously failed
        Then the service should:

        1. Acknowledge the historical failure state
        2. Prevent automatic retries of problematic jobs
        3. Preserve evidence of the original failure
        4. Require explicit intervention for reprocessing

        This is the failure-resilience scenario where:
        - The system demonstrates protective behavior against error loops
        - We avoid wasting resources on known problematic computations
        - Failed analyses remain visible for debugging
        - Engineers receive clear signals for needed intervention
        """
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
