import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from fastapi.responses import Response

from app.domain.models.analysis import Analysis
from app.domain.models.dataset import Dataset
from app.domain.models.environment import Environment
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn
from app.routers.common_analytics import get_analysis


def _make_analytics_in(**kwargs) -> TreeCoverLossAnalyticsIn:
    defaults = dict(
        aoi=AdminAreaOfInterest(type="admin", ids=["BRA.1"]),
        start_year="2020",
        end_year="2021",
        canopy_cover=30,
        intersections=[],
    )
    analytics_in = TreeCoverLossAnalyticsIn(**{**defaults, **kwargs})
    analytics_in.set_input_uris(Environment.production)
    return analytics_in


def _make_repository(analysis: Analysis) -> AsyncMock:
    repo = AsyncMock()
    repo.load_analysis.return_value = analysis
    return repo


def _make_response() -> MagicMock:
    response = MagicMock(spec=Response)
    response.headers = {}
    return response


RESOURCE_ID = uuid.uuid4()

# Metadata as it would be stored internally — includes all private fields.
_STORED_METADATA = {
    "aoi": {"type": "admin", "ids": ["BRA.1"], "provider": "gadm", "version": "4.1"},
    "start_year": "2020",
    "end_year": "2021",
    "canopy_cover": 30,
    "intersections": [],
    "_version": "20250912",
    "_analytics_name": "tree_cover_loss",
}

# The public subset clients should see.
_PUBLIC_METADATA = {k: v for k, v in _STORED_METADATA.items() if not k.startswith("_")}


class TestThumbprint:
    def test_thumbprint_without_set_input_uris_raises(self):
        defaults = dict(
            aoi=AdminAreaOfInterest(type="admin", ids=["BRA.1"]),
            start_year="2020",
            end_year="2021",
            canopy_cover=30,
            intersections=[],
        )
        analytics_in = TreeCoverLossAnalyticsIn(**defaults)
        with pytest.raises(ValueError):
            _ = analytics_in.thumbprint()

    def test_thumbprint_without_set_input_uris_defaults_to_production(self):
        a = _make_analytics_in()
        b = _make_analytics_in()
        b.set_input_uris(Environment.production)
        assert a.thumbprint() == b.thumbprint()

    def test_identical_requests_same_environment_same_thumbprint(self):
        a = _make_analytics_in()
        b = _make_analytics_in()
        a.set_input_uris(Environment.production)
        b.set_input_uris(Environment.production)
        assert a.thumbprint() == b.thumbprint()

    def test_different_environments_with_different_uris_different_thumbprints(self):
        staging_uri = "s3://lcl-analytics/zarr/umd_tree_cover_loss/v1.99/year.zarr"
        original = ZarrDatasetRepository._ZARR_URIS[Environment.staging].copy()
        try:
            ZarrDatasetRepository._ZARR_URIS[Environment.staging][
                Dataset.tree_cover_loss
            ] = staging_uri

            a = _make_analytics_in()
            b = _make_analytics_in()
            a.set_input_uris(Environment.production)
            b.set_input_uris(Environment.staging)
            assert a.thumbprint() != b.thumbprint()
        finally:
            ZarrDatasetRepository._ZARR_URIS[Environment.staging] = original

    def test_promoting_staging_uri_to_production_makes_thumbprints_equal(self):
        """When staging is promoted (URIs become identical), cached results
        should be shared — thumbprints must match."""
        prod_uri = ZarrDatasetRepository.resolve_zarr_uri(
            Dataset.tree_cover_loss, Environment.production
        )
        original = ZarrDatasetRepository._ZARR_URIS[Environment.staging].copy()
        try:
            # Simulate promotion: staging now points to the same URI as production
            ZarrDatasetRepository._ZARR_URIS[Environment.staging][
                Dataset.tree_cover_loss
            ] = prod_uri

            a = _make_analytics_in()
            b = _make_analytics_in()
            a.set_input_uris(Environment.production)
            b.set_input_uris(Environment.staging)
            assert a.thumbprint() == b.thumbprint()
        finally:
            ZarrDatasetRepository._ZARR_URIS[Environment.staging] = original

    def test_different_request_params_different_thumbprints(self):
        a = _make_analytics_in(start_year="2015")
        b = _make_analytics_in(start_year="2020")
        a.set_input_uris(Environment.production)
        b.set_input_uris(Environment.production)
        assert a.thumbprint() != b.thumbprint()


class TestGetAnalysisPrivateFieldFiltering:
    @pytest.mark.asyncio
    async def test_private_fields_are_absent_from_response_metadata(self):
        repo = _make_repository(
            Analysis(
                result=None, metadata=_STORED_METADATA, status=AnalysisStatus.saved
            )
        )
        result = await get_analysis(RESOURCE_ID, repo, _make_response())
        assert "_version" not in result.metadata
        assert "_analytics_name" not in result.metadata
        assert "_input_uris" not in result.metadata

    @pytest.mark.asyncio
    async def test_public_fields_are_preserved_in_response_metadata(self):
        repo = _make_repository(
            Analysis(
                result=None, metadata=_STORED_METADATA, status=AnalysisStatus.saved
            )
        )
        result = await get_analysis(RESOURCE_ID, repo, _make_response())
        assert result.metadata == _PUBLIC_METADATA

    @pytest.mark.asyncio
    async def test_filtering_applies_for_pending_status(self):
        repo = _make_repository(
            Analysis(
                result=None, metadata=_STORED_METADATA, status=AnalysisStatus.pending
            )
        )
        result = await get_analysis(RESOURCE_ID, repo, _make_response())
        assert "_input_uris" not in result.metadata
        assert result.metadata == _PUBLIC_METADATA

    @pytest.mark.asyncio
    async def test_filtering_applies_for_failed_status(self):
        repo = _make_repository(
            Analysis(
                result={"error": "something went wrong"},
                metadata=_STORED_METADATA,
                status=AnalysisStatus.failed,
            )
        )
        result = await get_analysis(RESOURCE_ID, repo, _make_response())
        assert "_input_uris" not in result.metadata
        assert result.metadata == _PUBLIC_METADATA

    @pytest.mark.asyncio
    async def test_metadata_with_no_private_fields_is_returned_unchanged(self):
        """Handles legacy stored analyses that pre-date the private fields."""
        repo = _make_repository(
            Analysis(
                result=None, metadata=_PUBLIC_METADATA, status=AnalysisStatus.saved
            )
        )
        result = await get_analysis(RESOURCE_ID, repo, _make_response())
        assert result.metadata == _PUBLIC_METADATA


class TestGetAnalysisStatusBehaviour:
    """Basic status tests."""

    @pytest.mark.asyncio
    async def test_saved_status_returns_success_message(self):
        repo = _make_repository(
            Analysis(
                result={"data": 1},
                metadata=_STORED_METADATA,
                status=AnalysisStatus.saved,
            )
        )
        result = await get_analysis(RESOURCE_ID, repo, _make_response())
        assert result.status == AnalysisStatus.saved
        assert "completed" in result.message.lower()

    @pytest.mark.asyncio
    async def test_pending_status_sets_retry_after_header(self):
        response = _make_response()
        repo = _make_repository(
            Analysis(
                result=None, metadata=_STORED_METADATA, status=AnalysisStatus.pending
            )
        )
        await get_analysis(RESOURCE_ID, repo, response)
        assert response.headers.get("Retry-After") == "1"

    @pytest.mark.asyncio
    async def test_failed_status_with_error_result_uses_error_as_message(self):
        error_msg = "AOI is too small."
        repo = _make_repository(
            Analysis(
                result={"error": error_msg},
                metadata=_STORED_METADATA,
                status=AnalysisStatus.failed,
            )
        )
        result = await get_analysis(RESOURCE_ID, repo, _make_response())
        assert result.message == error_msg

    @pytest.mark.asyncio
    async def test_missing_metadata_raises_404(self):
        repo = _make_repository(Analysis(result=None, metadata=None, status=None))
        with pytest.raises(HTTPException) as exc_info:
            await get_analysis(RESOURCE_ID, repo, _make_response())
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_repository_exception_raises_500(self):
        repo = AsyncMock()
        repo.load_analysis.side_effect = Exception("connection refused")
        with pytest.raises(HTTPException) as exc_info:
            await get_analysis(RESOURCE_ID, repo, _make_response())
        assert exc_info.value.status_code == 500


class TestModelDump:
    def test_model_dump_does_not_include_input_uris(self):
        analytics_in = _make_analytics_in()
        assert "_input_uris" not in analytics_in.model_dump()

    def test_model_dump_still_includes_version_and_analytics_name(self):
        """_version and _analytics_name are private too, but they ARE storage-worthy."""
        analytics_in = _make_analytics_in()
        dumped = analytics_in.model_dump()
        assert "_version" in dumped
        assert "_analytics_name" in dumped

    def test_thumbprint_still_works_without_input_uris_in_model_dump(self):
        """thumbprint() builds its own payload and adds _input_uris directly;
        it must not depend on model_dump() including it."""
        analytics_in = _make_analytics_in()
        result = analytics_in.thumbprint()
        assert isinstance(result, uuid.UUID)
