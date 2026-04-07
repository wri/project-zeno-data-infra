import uuid
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from app.analysis.common.analysis import FeatureTooSmallError
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.environment import Environment
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.models.common.analysis import AnalysisStatus, AnalyticsIn
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
)
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn
from app.use_cases.analysis.analysis_service import AnalysisService


@pytest.fixture
def stub_analysis_in():
    analytics_in = AnalyticsIn(
        aoi=ProtectedAreaOfInterest(
            type="protected_area",
            ids=["1234"],
        )
    )
    analytics_in.set_input_uris(Environment.production)
    return analytics_in


@pytest.fixture
def mock_analysis_repository():
    return MagicMock(spec=AnalysisRepository)


@pytest.fixture
def mock_analyzer():
    analyzer = MagicMock(spec=Analyzer)
    analyzer.analyze = AsyncMock()
    return analyzer


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
        expected_tp = analysis_service.resource_thumbprint()
        await analysis_service.do()

        ############
        # Assert   #
        ############
        mock_analysis_repository.load_analysis.assert_called_once_with(expected_tp)

        mock_analysis_repository_calls = (
            mock_analysis_repository.store_analysis.mock_calls
        )
        assert mock_analysis_repository.store_analysis.call_count == 3

        # assert that the new Analysis is
        # stored so it's available to clients immediately
        assert mock_analysis_repository_calls[0] == call(
            expected_tp,
            Analysis(None, stub_analysis_in.model_dump(), None),
        )

        # assert that the new Analysis is stored as pending before analysis starts
        assert mock_analysis_repository_calls[1] == call(
            expected_tp,
            Analysis(None, stub_analysis_in.model_dump(), AnalysisStatus.pending),
        )

        # assert that the saved Analysis result is stored at the end
        assert mock_analysis_repository_calls[2] == call(
            expected_tp,
            Analysis(None, stub_analysis_in.model_dump(), AnalysisStatus.saved),
        )

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
        expected_tp = analysis_service.resource_thumbprint()

        await analysis_service.do()

        ############
        # Assert   #
        ############
        mock_analysis_repository.load_analysis.assert_called_once_with(expected_tp)
        # assert result_thumbprint != stub_analysis_in.thumbprint(),
        # "Thumbprint only includes AnalyticsIn thumbprint"
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
        expected_tp = analysis_service.resource_thumbprint()
        await analysis_service.do()

        ############
        # Assert   #
        ############
        mock_analysis_repository.load_analysis.assert_called_once_with(expected_tp)
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
        expected_tp = analysis_service.resource_thumbprint()
        await analysis_service.do()

        ############
        # Assert   #
        ############
        mock_analysis_repository.load_analysis.assert_called_once_with(expected_tp)
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
        expected_tp = analysis_service.resource_thumbprint()
        await analysis_service.do()

        ############
        # Assert   #
        ############
        mock_analysis_repository.load_analysis.assert_called_once_with(expected_tp)
        mock_analyzer.analyze.assert_called()
        mock_analysis_repository.store_analysis.assert_called_with(
            expected_tp,
            Analysis(
                metadata=stub_analysis_in.model_dump(),
                result=None,
                status=AnalysisStatus.failed,
            ),
        )
        assert analysis_service.get_status() == AnalysisStatus.failed

    @pytest.mark.asyncio
    async def test_feature_too_small_error_stores_error_in_result(
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

        error_message = "AOI is too small. Please select a larger AOI."
        mock_analyzer.analyze.side_effect = [FeatureTooSmallError(error_message)]

        analysis_service = AnalysisService(
            analysis_repository=mock_analysis_repository,
            analyzer=mock_analyzer,
            event="test_endpoint_name",
        )

        ############
        # Act      #
        ############
        await analysis_service.set_resource_from(stub_analysis_in)
        expected_tp = analysis_service.resource_thumbprint()
        await analysis_service.do()

        ############
        # Assert   #
        ############
        mock_analyzer.analyze.assert_called()
        assert analysis_service.get_status() == AnalysisStatus.failed
        mock_analysis_repository.store_analysis.assert_called_with(
            expected_tp,
            Analysis(
                metadata=stub_analysis_in.model_dump(),
                result={"error": error_message},
                status=AnalysisStatus.failed,
            ),
        )


def _make_analyzer(thumbprint: uuid.UUID) -> MagicMock:
    analyzer = MagicMock(spec=Analyzer)
    analyzer.thumbprint.return_value = thumbprint
    return analyzer


def _make_analytics_in(**overrides) -> TreeCoverLossAnalyticsIn:
    defaults = dict(
        aoi=AdminAreaOfInterest(type="admin", ids=["BRA.1"]),
        start_year="2020",
        end_year="2021",
        canopy_cover=30,
        intersections=[],
    )
    analytics_in = TreeCoverLossAnalyticsIn(**{**defaults, **overrides})
    analytics_in.set_input_uris(Environment.production)
    return analytics_in


def _make_service(analyzer: Analyzer, analytics_in: AnalyticsIn) -> AnalysisService:
    repo = AsyncMock()
    repo.load_analysis.return_value = Analysis(
        result=None, metadata=analytics_in.model_dump(), status=None
    )
    return AnalysisService(
        analysis_repository=repo,
        analyzer=analyzer,
        event="test",
    )


class TestResourceThumbprint:
    @pytest.mark.asyncio
    async def test_resource_thumbprint_changes_when_analyzer_thumbprint_changes(self):
        """Same request, different analyzer thumbprint → different resource ID.
        Fails until resource_thumbprint() incorporates analyzer.thumbprint()."""
        analytics_in = _make_analytics_in()

        service_a = _make_service(
            _make_analyzer(uuid.uuid5(uuid.NAMESPACE_DNS, "v1")), analytics_in
        )
        await service_a.set_resource_from(analytics_in)

        service_b = _make_service(
            _make_analyzer(uuid.uuid5(uuid.NAMESPACE_DNS, "v2")), analytics_in
        )
        await service_b.set_resource_from(analytics_in)

        assert service_a.resource_thumbprint() != service_b.resource_thumbprint()

    @pytest.mark.asyncio
    async def test_resource_thumbprint_changes_when_request_params_change(self):
        """Same analyzer URIs, different request params → different resource ID."""
        analyzer = _make_analyzer(uuid.uuid5(uuid.NAMESPACE_DNS, "stable-uri"))
        analytics_in_a = _make_analytics_in(start_year="2021", end_year="2022")
        analytics_in_b = _make_analytics_in(start_year="2022", end_year="2023")

        service_a = _make_service(analyzer, analytics_in_a)
        await service_a.set_resource_from(analytics_in_a)

        service_b = _make_service(analyzer, analytics_in_b)
        await service_b.set_resource_from(analytics_in_b)

        assert service_a.resource_thumbprint() != service_b.resource_thumbprint()

    @pytest.mark.asyncio
    async def test_resource_thumbprint_stable_when_nothing_changes(self):
        """Determinism: identical inputs must always produce the same resource ID."""
        analyzer = _make_analyzer(uuid.uuid5(uuid.NAMESPACE_DNS, "stable-uri"))
        analytics_in = _make_analytics_in()

        service_a = _make_service(analyzer, analytics_in)
        await service_a.set_resource_from(analytics_in)

        service_b = _make_service(analyzer, analytics_in)
        await service_b.set_resource_from(analytics_in)

        assert service_a.resource_thumbprint() == service_b.resource_thumbprint()
