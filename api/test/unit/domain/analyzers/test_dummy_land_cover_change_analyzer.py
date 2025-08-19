from unittest.mock import AsyncMock
from uuid import UUID

import pytest
import pytest_asyncio
from app.domain.analyzers.dummy_land_cover_change_analyzer import (
    DummyLandCoverChangeAnalyzer,
)
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.land_cover import (
    LandCoverChangeAnalytics,
    LandCoverChangeAnalyticsIn,
)


class TestDummyLandCoverChangeAnalyzer:
    @pytest_asyncio.fixture(autouse=True)
    async def resource(self) -> LandCoverChangeAnalytics:
        """
        Test the compute method of LandCoverChangeService.
        """
        mock_analysis_repository = AsyncMock(spec=AnalysisRepository)

        analyzer = DummyLandCoverChangeAnalyzer(
            analysis_repository=mock_analysis_repository
        )

        analytics_in = LandCoverChangeAnalyticsIn(
            aoi={"type": "admin", "ids": ["BRA.12.1", "IDN.24.9"]},
        )

        analysis = Analysis(
            metadata=analytics_in.model_dump(),
            result=None,
            status=AnalysisStatus.pending,
        )

        await analyzer.analyze(analysis)
        return mock_analysis_repository

    @pytest.mark.asyncio
    async def test_compute_returns_result(self, resource: AsyncMock):
        resource.store_analysis.assert_called_once_with(
            resource_id=UUID("2b81bd83-3c0a-58b0-be23-5ed34122030d"),
            analytics=Analysis(
                metadata={
                    "aoi": {
                        "type": "admin",
                        "ids": ["BRA.12.1", "IDN.24.9"],
                        "provider": "gadm",
                        "version": "4.1",
                    }
                },
                result={
                    "id": [
                        "BRA.12.1",
                        "BRA.12.1",
                        "BRA.12.1",
                        "BRA.12.1",
                        "BRA.12.1",
                        "BRA.12.1",
                        "BRA.12.1",
                        "BRA.12.1",
                        "BRA.12.1",
                        "IDN.24.9",
                        "IDN.24.9",
                        "IDN.24.9",
                        "IDN.24.9",
                        "IDN.24.9",
                        "IDN.24.9",
                        "IDN.24.9",
                        "IDN.24.9",
                        "IDN.24.9",
                    ],
                    "land_cover_class_start": [
                        "Bare and sparse vegetation",
                        "Short vegetation",
                        "Tree cover",
                        "Wetland-short vegetation",
                        "Water",
                        "Snow/ice",
                        "Cropland",
                        "Built-up",
                        "Cultivated grasslands",
                        "Bare and sparse vegetation",
                        "Short vegetation",
                        "Tree cover",
                        "Wetland-short vegetation",
                        "Water",
                        "Snow/ice",
                        "Cropland",
                        "Built-up",
                        "Cultivated grasslands",
                    ],
                    "land_cover_class_end": [
                        "Cultivated grasslands",
                        "Built-up",
                        "Cropland",
                        "Snow/ice",
                        "Water",
                        "Wetland-short vegetation",
                        "Tree cover",
                        "Short vegetation",
                        "Bare and sparse vegetation",
                        "Cultivated grasslands",
                        "Built-up",
                        "Cropland",
                        "Snow/ice",
                        "Water",
                        "Wetland-short vegetation",
                        "Tree cover",
                        "Short vegetation",
                        "Bare and sparse vegetation",
                    ],
                    "area_ha": [
                        1.0,
                        2.0,
                        3.0,
                        4.0,
                        5.0,
                        6.0,
                        7.0,
                        8.0,
                        9.0,
                        1.0,
                        2.0,
                        3.0,
                        4.0,
                        5.0,
                        6.0,
                        7.0,
                        8.0,
                        9.0,
                    ],
                },
                status=AnalysisStatus.pending,
            ),
        )
