from typing import Dict

import newrelic.agent as nr_agent
import numpy as np

from app.domain.analyzers.analyzer import Analyzer
from app.domain.compute_engines.handlers.precalc_implementations.precalc_sql_query_builder import (
    PrecalcSqlQueryBuilder,
)
from app.domain.models.analysis import Analysis
from app.domain.models.dataset import (
    Dataset,
    DatasetAggregate,
    DatasetFilter,
    DatasetQuery,
)
from app.domain.models.environment import Environment
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
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
        # Should match result_uri in tcl_flow.py in pipelines.
        "admin_results_uri": "s3://lcl-analytics/zonal-statistics/tcl/v1.13/admin-tree-cover-loss_v20260518.parquet",
    },
}


async def build_query(analytics_in: TreeCoverLossAnalyticsIn) -> DatasetQuery:
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

    # if by fire, return both TCLF and non-fire TCL results by year
    # TODO: should this be by driver OR by fire?
    if "fire" in analytics_in.intersections:
        query.aggregate.datasets.append(Dataset.tree_cover_loss_from_fires)

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

    elif analytics_in.forest_filter == "intact_forest":
        query.filters.append(
            DatasetFilter(
                dataset=Dataset.intact_forest,
                op="=",
                value=1,
            )
        )
    return query


class TreeCoverLossAnalyzer(Analyzer):
    def __init__(self, compute_engine, input_uris: Dict[str, str] | None = None):
        self.compute_engine = compute_engine
        self.input_uris = input_uris

    @nr_agent.function_trace(name="TreeCoverLossAnalyzer.analyze")
    async def analyze(
        self, analysis: Analysis
    ) -> None:
        if self.input_uris is None:
            raise Exception("Input URIs must be provided for actual analysis")

        analytics_in = TreeCoverLossAnalyticsIn(**analysis.metadata)

        
        if analytics_in.aoi.type == "admin":
            results = await self.analyze_admin_areas(
                analytics_in
            )
        else:
            results = await self.analyze_otf(analytics_in)

        # postprocess, set NaN for carbon if canopy cover requested is <30
        if analytics_in.canopy_cover is not None and analytics_in.canopy_cover < 30:
            results[Dataset.carbon_emissions.get_field_name()] = np.nan

        analysis.result = results

    async def analyze_admin_areas(
        self,
        analytics_in: TreeCoverLossAnalyticsIn,
    ) -> Dict:
        if self.input_uris is None:
            raise Exception("Input URIs must be provided for actual analysis")
        query_service = DuckDbPrecalcQueryService(
            self.input_uris["admin_results_uri"]
        )

        query: DatasetQuery = await build_query(analytics_in)
        query_builder = PrecalcSqlQueryBuilder()
        sql_str: str = query_builder.build(analytics_in.aoi.ids, query)

        results: Dict = await query_service.execute(sql_str)

        results["aoi_type"] = ["admin"] * len(results["aoi_id"])

        return results

    async def analyze_otf(self, analytics_in: TreeCoverLossAnalyticsIn) -> Dict:
        query: DatasetQuery = await build_query(analytics_in)

        results = await self.compute_engine.compute(analytics_in.aoi, query)
        return results
