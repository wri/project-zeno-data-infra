from app.domain.analyzers.analyzer import Analyzer
from app.domain.compute_engines.compute_engine import ComputeEngine
from app.domain.models.analysis import Analysis
from app.domain.models.dataset import DatasetQuery, DatasetAggregate, Dataset, DatasetFilter
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn


class TreeCoverAnalyzer(Analyzer):
    def __init__(self, compute_engine: ComputeEngine):
        self.compute_engine = compute_engine

    async def analyze(self, analysis: Analysis):
        tree_cover_analytics_in = TreeCoverAnalyticsIn(**analysis.metadata)

        query = DatasetQuery(
            aggregate=DatasetAggregate(dataset=Dataset.area_hectares, func="sum"),
            group_bys=[],
            filters=[
                DatasetFilter(
                    dataset=Dataset.canopy_cover,
                    op=">=",
                    value=tree_cover_analytics_in.canopy_cover,
                ),
            ],
        )
        return await self.compute_engine.compute(
            tree_cover_analytics_in.aoi.type,
            tree_cover_analytics_in.aoi.ids,
            query
        )
