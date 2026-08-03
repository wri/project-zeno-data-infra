import asyncio
from typing import Any, Dict

import newrelic.agent as nr_agent
import pandas as pd

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

# mineral soil measures returned per aoi_id (single static snapshot, no year axis)
MINERAL_SOIL_MEASURES = (
    "gross_emissions_MgCO2e",
    "gross_removals_MgCO2",
    "net_flux_MgCO2e",
    "area_ha",
)

# vegetation years soil measures are annualized across (see analyze_mineral_soil,
# analyze_organic_soil): both soils are modeled at coarser-than-annual resolution
# (a single change interval, or two 5-year blocks) but are broadcast to match
# vegetation's per-year shape so consumers can treat all components uniformly.
ANNUALIZED_YEARS = tuple(range(2016, 2025))

# organic soil interval_end_year -> the vegetation years its block covers
ORGANIC_SOIL_INTERVAL_YEARS = {
    2020: tuple(range(2016, 2021)),
    2024: tuple(range(2021, 2025)),
}

INPUT_URIS = {
    Environment.staging: {},
    Environment.production: {
        "admin_vegetation_results_uri": (
            "s3://lcl-analytics/zonal-statistics/land_ghg_inventory-vegetation/"
            "global/admin-land_ghg_inventory-vegetation.parquet"
        ),
        "admin_agriculture_results_uri": (
            "s3://lcl-analytics/zonal-statistics/land_ghg_inventory-agriculture/"
            "v20260803/admin-land_ghg_inventory-agriculture.parquet"
        ),
        "admin_mineral_soil_results_uri": (
            "s3://lcl-analytics/zonal-statistics/land_ghg_inventory-mineral_soil/"
            "v20260729/admin-land_ghg_inventory-mineral_soil.parquet"
        ),
        "admin_organic_soil_results_uri": (
            "s3://lcl-analytics/zonal-statistics/land_ghg_inventory-organic_soil/"
            "v20260730/admin-land_ghg_inventory-organic_soil.parquet"
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
        (cropland, livestock) only - no year, removals, net flux, or area.
      - "mineral_soil": gross emissions / removals / net flux / area by
        aoi_id x year (2016-2024). The underlying data is a single static
        snapshot (the 2015-2020 SOC change interval); the same value is
        broadcast across every year for a vegetation-year-aligned shape.
      - "organic_soil": gross emissions / area by aoi_id x year (2016-2024).
        The underlying data has two 5-year blocks (covering 2016-2020 and
        2021-2024); each block's value is broadcast across its covered
        years."""

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
        vegetation, agriculture, mineral_soil, organic_soil = await asyncio.gather(
            self.analyze_vegetation(aoi_ids),
            self.analyze_agriculture(aoi_ids),
            self.analyze_mineral_soil(aoi_ids),
            self.analyze_organic_soil(aoi_ids),
        )
        analysis.result = {
            "vegetation": vegetation,
            "agriculture": agriculture,
            "mineral_soil": mineral_soil,
            "organic_soil": organic_soil,
        }

    async def analyze_vegetation(self, aoi_ids) -> Dict[str, Any]:
        columns = ("aoi_id", "land_state_class", "year") + VEGETATION_MEASURES
        result = await self._select(self.query_services["vegetation"], columns, aoi_ids)
        # vegetation parquet has no aoi_type column; every row is an admin area
        result["aoi_type"] = ["admin"] * len(result["aoi_id"])
        return result

    async def analyze_agriculture(self, aoi_ids) -> Dict[str, Any]:
        columns = ("aoi_id", "aoi_type", "category", "gross_emissions_MgCO2e")
        return await self._select(self.query_services["agriculture"], columns, aoi_ids)

    async def analyze_mineral_soil(self, aoi_ids) -> Dict[str, Any]:
        columns = ("aoi_id", "aoi_type") + MINERAL_SOIL_MEASURES
        result = await self._select(self.query_services["mineral_soil"], columns, aoi_ids)
        df = pd.DataFrame(result)
        df["year"] = [ANNUALIZED_YEARS] * len(df)
        return df.explode("year", ignore_index=True).to_dict(orient="list")

    async def analyze_organic_soil(self, aoi_ids) -> Dict[str, Any]:
        columns = (
            "aoi_id",
            "aoi_type",
            "interval_end_year",
            "gross_emissions_MgCO2e",
            "area_ha",
        )
        result = await self._select(self.query_services["organic_soil"], columns, aoi_ids)
        df = pd.DataFrame(result)
        df["year"] = df["interval_end_year"].map(ORGANIC_SOIL_INTERVAL_YEARS)
        df = df.explode("year", ignore_index=True).drop(columns="interval_end_year")
        return df.to_dict(orient="list")

    @staticmethod
    async def _select(query_service, columns, aoi_ids) -> Dict[str, Any]:
        id_str = ", ".join([f"'{aoi_id}'" for aoi_id in aoi_ids])
        column_list = ", ".join(columns)
        query = f"select {column_list} from data_source where aoi_id in ({id_str})"
        return await query_service.execute(query)
