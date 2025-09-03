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
                    op="in",
                    value=self._build_years(
                        analytics_in.start_year, analytics_in.end_year
                    ),
                )
            ],
        )

        return await self.compute_engine.compute(
            analytics_in.aoi.type, analytics_in.aoi.ids, query
        )

    def _build_years(self, start_year: str, end_year: str):
        start = int(start_year)
        end = int(end_year)

        year_ranges = [f"'{year}-{year + 5}'" for year in range(start, end, 5)]
        return f"({', '.join(year_ranges)})"
