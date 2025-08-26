from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.dataset import (
    Dataset,
    DatasetAggregate,
    DatasetFilter,
    DatasetQuery,
)
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn


class TreeCoverLossAnalyzer(Analyzer):
    def __init__(self, compute_engine):
        self.compute_engine = compute_engine

    async def analyze(self, analysis: Analysis):
        analytics_in = TreeCoverLossAnalyticsIn(**analysis.metadata)

        query = DatasetQuery(
            aggregate=DatasetAggregate(dataset=Dataset.area_hectares, func="sum"),
            group_bys=[Dataset.tree_cover_loss],
            filters=[
                DatasetFilter(
                    dataset=Dataset.canopy_cover,
                    op=">=",
                    value=analytics_in.canopy_cover,
                ),
                DatasetFilter(
                    dataset=Dataset.tree_cover_loss,
                    op=">=",
                    value=analytics_in.start_year,
                ),
                DatasetFilter(
                    dataset=Dataset.tree_cover_loss,
                    op="<=",
                    value=analytics_in.end_year,
                ),
            ],
        )
        return await self.compute_engine.compute(
            analytics_in.aoi.type, analytics_in.aoi.ids, query
        )
