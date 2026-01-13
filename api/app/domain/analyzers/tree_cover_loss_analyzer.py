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
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn


class TreeCoverLossAnalyzer(Analyzer):
    def __init__(self, compute_engine):
        self.compute_engine = compute_engine

    @nr_agent.function_trace(name="TreeCoverLossAnalyzer.analyze")
    async def analyze(self, analysis: Analysis):
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
            query.filters.append(
                DatasetFilter(
                    dataset=Dataset.natural_lands,
                    op="in",
                    value=[2, 5, 8, 9],  # SBTN classes we define as "natural forest"
                )
            )

        results = await self.compute_engine.compute(
            analytics_in.aoi.type, analytics_in.aoi.ids, query
        )

        # postprocess, set NaN for carbon if canopy cover requested is <30
        if analytics_in.canopy_cover is not None and analytics_in.canopy_cover < 30:
            results[Dataset.carbon_emissions.get_field_name()] = np.nan

        return results
