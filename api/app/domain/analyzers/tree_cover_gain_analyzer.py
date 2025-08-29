from app.domain.analyzers.analyzer import Analyzer
from app.domain.compute_engines.compute_engine import ComputeEngine
from app.domain.models.analysis import Analysis
from app.domain.models.dataset import (
    Dataset,
    DatasetAggregate,
    DatasetFilter,
    DatasetQuery,
)
from app.models.land_change.tree_cover_gain import TreeCoverGainAnalyticsIn


class TreeCoverGainAnalyzer(Analyzer):
    def __init__(self, compute_engine: ComputeEngine):
        self.compute_engine = compute_engine

    async def analyze(self, analysis: Analysis):
        analytics_in = TreeCoverGainAnalyticsIn(**analysis.metadata)
        query = DatasetQuery(
            aggregate=DatasetAggregate(dataset=Dataset.area_hectares, func="sum"),
            group_bys=[Dataset.tree_cover_gain],
            filters=[
                DatasetFilter(
                    dataset=Dataset.tree_cover_gain,
                    op=">=",
                    value="2000",
                ),
                DatasetFilter(
                    dataset=Dataset.tree_cover_gain,
                    op="<=",
                    value="2005",
                ),
            ],
        )

        return await self.compute_engine.compute(
            analytics_in.aoi.type, analytics_in.aoi.ids, query
        )
