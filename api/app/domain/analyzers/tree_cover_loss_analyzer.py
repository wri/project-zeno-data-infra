from typing import Dict

import newrelic.agent as nr_agent
import numpy as np

from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.dataset import (
    Dataset,
    DatasetAggregate,
    DatasetFilter,
    DatasetQuery,
)
from app.domain.models.environment import Environment
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn

INPUT_URIS: Dict[Environment, Dict[str, str]] = {
    Environment.staging: {
        **{
            str(ds): ZarrDatasetRepository.resolve_zarr_uri(ds, Environment.staging)
            for ds in [
                Dataset.area_hectares,
                Dataset.canopy_cover,
                Dataset.carbon_emissions,
                Dataset.natural_forests,
                Dataset.primary_forest,
                Dataset.tree_cover_loss,
                Dataset.tree_cover_loss_drivers,
            ]
        },
        # Should match result_uri in tcl_flow.py in pipelines.
        "admin_results_uri": "s3://lcl-analytics/zonal-statistics/tcl/v1.13/admin-tree-cover-loss_v20260424.parquet",
    },
    Environment.production: {
        **{
            str(ds): ZarrDatasetRepository.resolve_zarr_uri(ds, Environment.production)
            for ds in [
                Dataset.area_hectares,
                Dataset.canopy_cover,
                Dataset.carbon_emissions,
                Dataset.natural_forests,
                Dataset.primary_forest,
                Dataset.tree_cover_loss,
                Dataset.tree_cover_loss_drivers,
            ]
        },
        "admin_results_uri": "s3://lcl-analytics/zonal-statistics/tcl/v1.12/admin-tree-cover-loss.parquet",
    },
}


class TreeCoverLossAnalyzer(Analyzer):
    def __init__(self, compute_engine, input_uris: Dict[str, str] | None = None):
        self.compute_engine = compute_engine
        self.input_uris = input_uris

    @nr_agent.function_trace(name="TreeCoverLossAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        if self.input_uris is None:
            raise Exception("Input URIs must be provided for actual analysis")

        analytics_in = TreeCoverLossAnalyticsIn(**analysis.metadata)

        query = DatasetQuery(
            aggregate=DatasetAggregate(
                datasets=[Dataset.area_hectares, Dataset.carbon_emissions], func="sum"
            ),
            group_bys=[],
            filters=[
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

        # if by driver, return across all years since that's how the model is calculated
        # otherwise group by TCL year
        if "driver" in analytics_in.intersections:
            query.group_bys.append(Dataset.tree_cover_loss_drivers)
        else:
            query.group_bys.append(Dataset.tree_cover_loss)

        if analytics_in.canopy_cover is not None:
            query.filters.append(
                DatasetFilter(
                    dataset=Dataset.canopy_cover,
                    op=">=",
                    value=analytics_in.canopy_cover,
                ),
            )

        if analytics_in.forest_filter == "primary_forest":
            query.filters.append(
                DatasetFilter(
                    dataset=Dataset.primary_forest,
                    op="=",
                    value=1,
                )
            )
        elif analytics_in.forest_filter == "natural_forest":
            query.group_bys.append(Dataset.natural_forests)

        results = await self.compute_engine.compute(analytics_in.aoi, query)

        # postprocess, set NaN for carbon if canopy cover requested is <30
        if analytics_in.canopy_cover is not None and analytics_in.canopy_cover < 30:
            results[Dataset.carbon_emissions.get_field_name()] = np.nan

        analysis.result = results
