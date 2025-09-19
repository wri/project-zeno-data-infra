from typing import List

import newrelic.agent as nr_agent
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

    @nr_agent.function_trace(name="TreeCoverGainAnalyzer.analyze")
    async def analyze(self, analysis: Analysis):
        analytics_in = TreeCoverGainAnalyticsIn(**analysis.metadata)

        filters: List[DatasetFilter] = [
            DatasetFilter(
                dataset=Dataset.tree_cover_gain,
                op="in",
                value=self._build_years(analytics_in.start_year, analytics_in.end_year),
            )
        ]

        if analytics_in.forest_filter is not None:
            if analytics_in.forest_filter == "primary_forest":
                filters.append(
                    DatasetFilter(
                        dataset=Dataset.primary_forest,
                        op="=",
                        value=True,
                    )
                )

        query = DatasetQuery(
            aggregate=DatasetAggregate(datasets=[Dataset.area_hectares], func="sum"),
            group_bys=[Dataset.tree_cover_gain],
            filters=filters,
        )

        return await self.compute_engine.compute(
            analytics_in.aoi.type, analytics_in.aoi.ids, query
        )

    def _build_years(self, start_year: str, end_year: str):
        """
        Build a tuple of strings representing five-year periods between start_year and end_year.

        Args:
            start_year: Start year as string (multiple of five, >= "2000").
            end_year: End year as string (multiple of five, > start_year).

        Returns:
            A tuple of periods (strings), e.g. ('2000-2005', '2005-2010')
        """
        start = int(start_year)
        end = int(end_year)

        year_ranges = [f"{year}-{year + 5}" for year in range(start, end, 5)]

        return tuple(year_ranges)
