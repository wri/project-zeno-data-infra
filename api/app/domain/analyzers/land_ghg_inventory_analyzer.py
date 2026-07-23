from typing import Any, Dict

import newrelic.agent as nr_agent

from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.environment import Environment
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.models.land_change.land_ghg_inventory import LandGHGInventoryAnalyticsIn

# measures returned per (aoi_id, land_state_class, year)
MEASURES = (
    "gross_emissions_MgCO2e",
    "gross_removals_MgCO2",
    "net_flux_MgCO2e",
    "area_ha",
)

INPUT_URIS = {
    Environment.staging: {},
    Environment.production: {
        "admin_results_uri": (
            "s3://lcl-analytics/zonal-statistics/land_ghg_inventory-vegetation/"
            "global/admin-land_ghg_inventory-vegetation.parquet"
        ),
    },
}


class LandGHGInventoryAnalyzer(Analyzer):
    """Vegetation GHG flux (gross emissions / gross removals / net flux) and area by
    land_state_class x year for admin areas (by aoi_id), read from the precomputed
    zonal-statistics parquet. Admin areas only, no on-the-fly computation."""

    def __init__(self, input_uris: Dict[str, str] | None = None):
        self.input_uris = input_uris

    @nr_agent.function_trace(name="LandGHGInventoryAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        if self.input_uris is None:
            raise Exception("Input URIs must be provided for actual analysis")

        analytics_in = LandGHGInventoryAnalyticsIn(**analysis.metadata)
        analysis.result = await self.analyze_admin_areas(analytics_in.aoi.ids)

    async def analyze_admin_areas(self, aoi_ids) -> Dict[str, Any]:
        if self.input_uris is None:
            raise Exception("Input URIs must be provided for actual analysis")

        id_str = (", ").join([f"'{aoi_id}'" for aoi_id in aoi_ids])
        columns = ", ".join(("aoi_id", "land_state_class", "year") + MEASURES)
        query = f"select {columns} from data_source where aoi_id in ({id_str})"

        query_service = DuckDbPrecalcQueryService(self.input_uris["admin_results_uri"])
        result = await query_service.execute(query)
        result["aoi_type"] = ["admin"] * len(result["aoi_id"])

        return result
