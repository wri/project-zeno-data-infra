import asyncio
from typing import Any, Dict

import newrelic.agent as nr_agent

from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.environment import Environment
from app.models.land_change.land_ghg_inventory import LandGHGInventoryAnalyticsIn

# vegetation measures returned per (aoi_id, land_state_class, year)
VEGETATION_MEASURES = (
    "gross_emissions_MgCO2e",
    "gross_removals_MgCO2",
    "net_flux_MgCO2e",
    "area_ha",
)

INPUT_URIS = {
    Environment.staging: {},
    Environment.production: {
        "admin_vegetation_results_uri": (
            "s3://lcl-analytics/zonal-statistics/land_ghg_inventory-vegetation/"
            "global/admin-land_ghg_inventory-vegetation.parquet"
        ),
        "admin_agriculture_results_uri": (
            "s3://lcl-analytics/zonal-statistics/land_ghg_inventory-agriculture/"
            "v20260727/admin-land_ghg_inventory-agriculture.parquet"
        ),
    },
}


class LandGHGInventoryAnalyzer(Analyzer):
    """Land GHG inventory for admin areas (by aoi_id), read from precomputed
    zonal-statistics parquets. Admin areas only, no on-the-fly computation.

    The result holds one table per aggregation category, each aggregated
    differently:
      - "vegetation": gross emissions / removals / net flux / area by
        land_state_class x year.
      - "agriculture": a coarse snapshot of gross emissions by category
        (cropland / livestock) only - no year, removals, net flux, or area.
    "soil" is added as a further key later."""

    def __init__(
        self,
        query_services: Dict[str, Any] | None = None,
        input_uris: Dict[str, str] | None = None,
    ):
        self.query_services = query_services or {}
        self.input_uris = input_uris

    @nr_agent.function_trace(name="LandGHGInventoryAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        if self.input_uris is None:
            raise Exception("Input URIs must be provided for actual analysis")

        analytics_in = LandGHGInventoryAnalyticsIn(**analysis.metadata)
        aoi_ids = analytics_in.aoi.ids
        vegetation, agriculture = await asyncio.gather(
            self.analyze_vegetation(aoi_ids),
            self.analyze_agriculture(aoi_ids),
        )
        analysis.result = {"vegetation": vegetation, "agriculture": agriculture}

    async def analyze_vegetation(self, aoi_ids) -> Dict[str, Any]:
        columns = ("aoi_id", "land_state_class", "year") + VEGETATION_MEASURES
        result = await self._select(self.query_services["vegetation"], columns, aoi_ids)
        # vegetation parquet has no aoi_type column; every row is an admin area
        result["aoi_type"] = ["admin"] * len(result["aoi_id"])
        return result

    async def analyze_agriculture(self, aoi_ids) -> Dict[str, Any]:
        columns = ("aoi_id", "aoi_type", "category", "gross_emissions_MgCO2e")
        return await self._select(self.query_services["agriculture"], columns, aoi_ids)

    @staticmethod
    async def _select(query_service, columns, aoi_ids) -> Dict[str, Any]:
        id_str = ", ".join([f"'{aoi_id}'" for aoi_id in aoi_ids])
        column_list = ", ".join(columns)
        query = f"select {column_list} from data_source where aoi_id in ({id_str})"
        return await query_service.execute(query)
