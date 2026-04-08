from typing import Dict, List

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
from app.domain.models.environment import Environment
from app.models.land_change.tree_cover_gain import TreeCoverGainAnalyticsIn

INPUT_URIS = {
    Environment.staging: {},
    Environment.production: {
        "area_hectares": "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area_ha.zarr",
        "tree_cover_gain": "s3://gfw-data-lake/umd_tree_cover_gain_from_height/v20240126/raster/epsg-4326/zarr/period.zarr",
        "primary_forest": "s3://gfw-data-lake/umd_regional_primary_forest_2001/v201901/raster/epsg-4326/zarr/is.zarr",
        "admin_results_uri": "s3://lcl-analytics/zonal-statistics/admin-tree-cover-gain.parquet",
    },
}


class TreeCoverGainAnalyzer(Analyzer):
    def __init__(
        self, compute_engine: ComputeEngine, input_uris: Dict[str, str] | None = None
    ):
        self.compute_engine = compute_engine
        self.input_uris = input_uris

    @nr_agent.function_trace(name="TreeCoverGainAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        if self.input_uris is None:
            raise RuntimeError("Input URIs must be provided for actual analysis")

        analytics_in = TreeCoverGainAnalyticsIn(**analysis.metadata)
        if analysis.metadata.get("_input_uris") is not None:
            analytics_in._input_uris = analysis.metadata["_input_uris"]

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

        analysis.result = await self.compute_engine.compute(analytics_in.aoi, query)

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
