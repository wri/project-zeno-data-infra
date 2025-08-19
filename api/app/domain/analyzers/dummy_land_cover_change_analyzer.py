from typing import List

from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.models.land_change.land_cover import (
    LandCoverChangeAnalyticsIn,
    LandCoverChangeResult,
)

LAND_COVER_CLASSES = [
    "Bare and sparse vegetation",
    "Short vegetation",
    "Tree cover",
    "Wetland-short vegetation",
    "Water",
    "Snow/ice",
    "Cropland",
    "Built-up",
    "Cultivated grasslands",
]


class DummyLandCoverChangeAnalyzer(Analyzer):
    def __init__(self, analysis_repository: AnalysisRepository):
        self.analysis_repository = analysis_repository

    async def analyze(self, analysis: Analysis):
        land_cover_change_analytics_in = LandCoverChangeAnalyticsIn(**analysis.metadata)
        aoi_ids: List[str] = []
        land_cover_start: List[str] = []
        land_cover_end: List[str] = []
        area: List[float] = []

        for aoi_id in analysis.metadata["aoi"]["ids"]:
            aoi_ids += [aoi_id] * len(LAND_COVER_CLASSES)
            land_cover_start += LAND_COVER_CLASSES
            land_cover_end += reversed(LAND_COVER_CLASSES)
            area += range(1, len(LAND_COVER_CLASSES) + 1)

        results = LandCoverChangeResult(
            id=aoi_ids,
            land_cover_class_start=land_cover_start,
            land_cover_class_end=land_cover_end,
            area_ha=area,
        ).model_dump()

        await self.analysis_repository.store_analysis(
            resource_id=land_cover_change_analytics_in.thumbprint(),
            analytics=Analysis(
                metadata=analysis.metadata,
                result=results,
                status=analysis.status,
            ),
        )
