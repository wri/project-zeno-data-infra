from typing import List

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
        groupbys = []

        filters: List[DatasetFilter] = [
            DatasetFilter(
                dataset=Dataset.canopy_cover,
                op=">=",
                value=tree_cover_analytics_in.canopy_cover,
            ),
        ]

        if tree_cover_analytics_in.forest_filter is not None:
            forest_filter = Dataset.primary_forest
            if tree_cover_analytics_in.forest_filter == Dataset.intact_forest.value:
                forest_filter = Dataset.intact_forest

            filters.append(
                DatasetFilter(
                    dataset=forest_filter,
                    op="=",
                    value=True,
                )
            )

        query = DatasetQuery(
            aggregate=DatasetAggregate(dataset=Dataset.area_hectares, func="sum"),
            group_bys=groupbys,
            filters=filters
        )
        return await self.compute_engine.compute(
            tree_cover_analytics_in.aoi.type,
            tree_cover_analytics_in.aoi.ids,
            query
        )
