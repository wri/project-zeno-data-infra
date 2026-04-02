import json
import uuid
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
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn

DATASETS = [
    Dataset.area_hectares,
    Dataset.canopy_cover,
    Dataset.primary_forest,
]


class TreeCoverAnalyzer(Analyzer):
    def __init__(
        self,
        compute_engine: ComputeEngine,
        input_uris: Dict[str, str] = None,
    ):
        self.compute_engine = compute_engine
        self.input_uris = input_uris

    @nr_agent.function_trace(name="TreeCoverAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        tree_cover_analytics_in = TreeCoverAnalyticsIn(**analysis.metadata)

        groupbys: List[Dataset] = []

        filters: List[DatasetFilter] = [
            DatasetFilter(
                dataset=Dataset.canopy_cover,
                op=">=",
                value=tree_cover_analytics_in.canopy_cover,
            ),
        ]

        if tree_cover_analytics_in.forest_filter is not None:
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
        analysis.result = await self.compute_engine.compute(
            tree_cover_analytics_in.aoi, query
        )

    def thumbprint(self):
        return uuid.uuid5(
            uuid.NAMESPACE_DNS, json.dumps(self.input_uris, sort_keys=True)
        )
