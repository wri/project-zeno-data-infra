from random import random

from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.models.land_change.tree_cover_gain import TreeCoverGainAnalyticsIn


class DummyTreeCoverGainAnalyzer(Analyzer):
    async def analyze(self, analysis: Analysis):
        land_cover_change_analytics_in = TreeCoverGainAnalyticsIn(**analysis.metadata)
        years = list(
            range(
                int(land_cover_change_analytics_in.start_year),
                int(land_cover_change_analytics_in.end_year) + 1,
                5,
            )
        )

        return {
            "aoi_id": analysis.metadata["aoi"]["ids"] * len(years),
            "aoi_type": land_cover_change_analytics_in.aoi.type * len(years),
            "tree_cover_gain_year": years,
            "area_ha": [(random() * 100) for _ in years],
        }
