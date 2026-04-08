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
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn

INPUT_URIS = {
    Environment.staging: {},
    Environment.production: {
        **{
            str(ds): ZarrDatasetRepository.resolve_zarr_uri(ds, Environment.production)
            for ds in [
                Dataset.area_hectares,
                Dataset.canopy_cover,
                Dataset.primary_forest,
            ]
        },
        "admin_results_uri": "s3://lcl-analytics/zonal-statistics/admin-tree-cover.parquet",
    },
}


class TreeCoverAnalyzer(Analyzer):
    def __init__(
        self, compute_engine: ComputeEngine, input_uris: Dict[str, str]
    ) -> None:
        self.compute_engine = compute_engine
        self.input_uris = input_uris

    @nr_agent.function_trace(name="TreeCoverAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        if self.input_uris is None:
            raise Exception("Input URIs must be provided for actual analysis")

        analytics_in = TreeCoverAnalyticsIn(**analysis.metadata)
        if analysis.metadata.get("_input_uris") is not None:
            analytics_in._input_uris = analysis.metadata["_input_uris"]
        groupbys: List[Dataset] = []

        filters: List[DatasetFilter] = [
            DatasetFilter(
                dataset=Dataset.canopy_cover,
                op=">=",
                value=analytics_in.canopy_cover,
            ),
        ]

        if analytics_in.forest_filter is not None:
            forest_filter = Dataset.primary_forest
            filters.append(
                DatasetFilter(
                    dataset=forest_filter,
                    op="=",
                    value=True,
                )
            )

        query = DatasetQuery(
            aggregate=DatasetAggregate(datasets=[Dataset.area_hectares], func="sum"),
            group_bys=groupbys,
            filters=filters,
        )
        analysis.result = await self.compute_engine.compute(analytics_in.aoi, query)
