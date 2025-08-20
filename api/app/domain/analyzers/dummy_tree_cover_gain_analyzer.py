from random import random
from typing import Dict

from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.models.land_change.tree_cover_gain import TreeCoverGainAnalyticsIn


class DummyTreeCoverGainAnalyzer(Analyzer):
    def __init__(self, analysis_repository: AnalysisRepository):
        self.analysis_repository = analysis_repository

    async def analyze(self, analysis: Analysis):
        land_cover_change_analytics_in = TreeCoverGainAnalyticsIn(**analysis.metadata)
        years = list(
            range(
                int(land_cover_change_analytics_in.start_year),
                int(land_cover_change_analytics_in.end_year) + 1,
                5,
            )
        )
        results: Dict = {
            "id": analysis.metadata["aoi"]["ids"] * len(years),
            "tree_cover_gain__year": years,
            "tree_cover_gain_area__ha": [(random() * 100) for _ in years],
        }
        await self.analysis_repository.store_analysis(
            resource_id=land_cover_change_analytics_in.thumbprint(),
            analytics=Analysis(
                metadata=analysis.metadata,
                result=results,
                status=analysis.status,
            ),
        )
