import pandas as pd
import pytest
import pytest_asyncio
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.land_cover import (
    LandCoverChangeAnalytics,
    LandCoverChangeAnalyticsIn,
)
from app.use_cases.analysis.land_cover.land_cover_change import LandCoverChangeService

pytest.mark.xfail(reason="this was for the old implementation, needs to be updated.")
class TestLandCoverChangeServiceCompute:
    @pytest_asyncio.fixture(autouse=True)
    async def resource(self) -> LandCoverChangeAnalytics:
        """
        Test the compute method of LandCoverChangeService.
        """
        service = LandCoverChangeService()
        analytics_in = LandCoverChangeAnalyticsIn(
            aoi={"type": "admin", "ids": ["BRA.12.1", "IDN.24.9"]},
        )

        resource: LandCoverChangeAnalytics = await service.compute(analytics_in)
        return resource

    @pytest.mark.asyncio
    async def test_compute_returns_correct_status(
        self, resource: LandCoverChangeAnalytics
    ):
        assert (
            resource.status == AnalysisStatus.saved
        ), "Expected status to be 'saved' after computation."

    @pytest.mark.asyncio
    async def test_compute_returns_metadata(self, resource: LandCoverChangeAnalytics):
        assert (
            resource.metadata is not None
        ), "Expected metadata to be present after computation."

    @pytest.mark.asyncio
    async def test_compute_returns_result(self, resource: LandCoverChangeAnalytics):
        assert (
            resource.result is not None
        ), "Expected result to be present after computation."

    @pytest.mark.asyncio
    async def test_compute_result_contains_ids(
        self, resource: LandCoverChangeAnalytics
    ):
        assert resource.result is not None
        assert "BRA.12.1" in resource.result.id, "Expected result to contain AOI IDs."
        assert "IDN.24.9" in resource.result.id, "Expected result to contain AOI IDs."

    @pytest.mark.asyncio
    async def test_compute_result_is_valid_dataframe(
        self, resource: LandCoverChangeAnalytics
    ):
        assert resource.result is not None

        # If resource.result is a Pydantic model, convert it to dict first
        result_dict = resource.result.dict()
        df = pd.DataFrame([result_dict])
        assert not df.empty, "Expected result data to be a non-empty DataFrame."
