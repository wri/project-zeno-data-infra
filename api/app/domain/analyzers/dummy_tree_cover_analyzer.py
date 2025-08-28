from random import random

from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn


class DummyTreeCoverAnalyzer(Analyzer):
    async def analyze(self, analysis: Analysis):
        land_cover_change_analytics_in = TreeCoverAnalyticsIn(**analysis.metadata)

        return {
            "aoi_id": analysis.metadata["aoi"]["ids"] ,
            "aoi_type": [
                land_cover_change_analytics_in.aoi.type
                for _ in analysis.metadata["aoi"]["ids"]
            ],
            "area_ha": [
                random() * 100
                for _ in analysis.metadata["aoi"]["ids"]
            ]
        }
