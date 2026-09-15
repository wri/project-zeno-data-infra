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

# vegetation years agriculture and soil measures are annualized across (see
# analyze_agriculture, analyze_mineral_soil, analyze_organic_soil): all three
# are modeled at coarser-than-annual resolution (a single snapshot, a single
# change interval, or two 5-year blocks) but are broadcast to match
# vegetation's per-year shape so consumers can treat all components uniformly.
ANNUALIZED_YEARS = tuple(range(2016, 2025))

# organic soil interval_end_year -> the vegetation years its block covers
ORGANIC_SOIL_INTERVAL_YEARS = {
    2020: tuple(range(2016, 2021)),
    2024: tuple(range(2021, 2025)),
}

# The single row a global analysis collapses to.
GLOBAL_AOI_ID = "GLOBAL"

# Top (country) rollup tier only. Each parquet also holds the adm1 and adm2
# rollups of the same pixels (see rollup_by_gadm_and_convert_to_aoi in
# pipelines/prefect_flows/common_stages.py), so an unfiltered SUM would count
# every value three times.
ADM0_ONLY = "aoi_id not like '%.%'"


def admin_query(dimensions, measures, aoi_ids) -> str:
    """Rows for the named admin areas, one row per area."""
    id_str = ", ".join([f"'{aoi_id}'" for aoi_id in aoi_ids])
    # aoi_type is emitted as a literal rather than read from the parquet,
    # where it is unconditionally 'admin' (and absent altogether from the
    # vegetation table).
    columns = ", ".join(("aoi_id", "'admin' as aoi_type", *dimensions, *measures))
    return f"select {columns} from data_source where aoi_id in ({id_str})"


def global_query(dimensions, measures) -> str:
    """The country tier summed into a single world row.

    Grouped by exactly the dimensions the caller reshapes on afterwards, so
    the row count matches what one admin area returns and the downstream year
    broadcast stays correct.
    """
    columns = ", ".join(
        (
            f"'{GLOBAL_AOI_ID}' as aoi_id",
            "'global' as aoi_type",
            *dimensions,
            *[f"sum({m}) as {m}" for m in measures],
        )
    )
    query = f"select {columns} from data_source where {ADM0_ONLY}"
    if dimensions:
        query += f" group by {', '.join(dimensions)}"
    return query


def _broadcast_years(result: Dict[str, Any]) -> Dict[str, Any]:
    """Repeat each row across every vegetation year.

    The agriculture and mineral soil tables have no year axis of their own, so
    the SUM behind a global row must already have happened here -- broadcasting
    first and aggregating after would multiply the world total by the number of
    years.
    """
    if not result["aoi_id"]:
        return {key: [] for key in (*result, "year")}
    df = pd.DataFrame(result)
    df["year"] = [ANNUALIZED_YEARS] * len(df)
    return df.explode("year", ignore_index=True).to_dict(orient="list")


INPUT_URIS = {
    Environment.staging: {},
    Environment.production: {
        "admin_vegetation_results_uri": (
            "s3://lcl-analytics/zonal-statistics/land_ghg_inventory-vegetation/"
            "global/admin-land_ghg_inventory-vegetation.parquet"
        ),
        "admin_agriculture_results_uri": (
            "s3://lcl-analytics/zonal-statistics/land_ghg_inventory-agriculture/"
            "v20260908/admin-land_ghg_inventory-agriculture.parquet"
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
    """Land GHG inventory for admin areas (by aoi_id) or for the whole world,
    read from precomputed zonal-statistics parquets. No on-the-fly computation.

    A global AOI returns the same tables with the same dimensions, summed over
    the country tier into a single row set carrying aoi_id "GLOBAL" -- one
    world figure per land_state_class/category/year, not a per-country
    breakdown.

    The result holds one table per aggregation category, each aggregated
    differently:
      - "vegetation": gross emissions / removals / net flux / area by
        land_state_class x year.
      - "agriculture": gross emissions by aoi_id x category (cropland,
        livestock) x year (2016-2024). The underlying data is a single
        static snapshot; the same value is broadcast across every year for
        a vegetation-year-aligned shape. No removals, net flux, or area.
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
        aoi = analytics_in.aoi
        vegetation, agriculture, mineral_soil, organic_soil = await asyncio.gather(
            self.analyze_vegetation(aoi),
            self.analyze_agriculture(aoi),
            self.analyze_mineral_soil(aoi),
            self.analyze_organic_soil(aoi),
        )
        analysis.result = {
            "vegetation": vegetation,
            "agriculture": agriculture,
            "mineral_soil": mineral_soil,
            "organic_soil": organic_soil,
        }

    async def analyze_vegetation(self, aoi) -> Dict[str, Any]:
        return await self._select(
            "vegetation", aoi, ("land_state_class", "year"), VEGETATION_MEASURES
        )

    async def analyze_agriculture(self, aoi) -> Dict[str, Any]:
        result = await self._select(
            "agriculture", aoi, ("category",), ("gross_emissions_MgCO2e",)
        )
        return _broadcast_years(result)

    async def analyze_mineral_soil(self, aoi) -> Dict[str, Any]:
        result = await self._select("mineral_soil", aoi, (), MINERAL_SOIL_MEASURES)
        return _broadcast_years(result)

    async def analyze_organic_soil(self, aoi) -> Dict[str, Any]:
        result = await self._select(
            "organic_soil",
            aoi,
            ("interval_end_year",),
            ("gross_emissions_MgCO2e", "area_ha"),
        )
        if not result["aoi_id"]:
            return {key: [] for key in (*result, "year") if key != "interval_end_year"}
        df = pd.DataFrame(result)
        df["year"] = df["interval_end_year"].map(ORGANIC_SOIL_INTERVAL_YEARS)
        df = df.explode("year", ignore_index=True).drop(columns="interval_end_year")
        return df.to_dict(orient="list")

    async def _select(self, component, aoi, dimensions, measures) -> Dict[str, Any]:
        """Read one component's table for `aoi`, at the grain the caller names.

        The only place the admin/global split is decided: a global AOI sums
        the country tier into one world row, an admin AOI reads its rows back
        directly. Both shapes carry the same columns, so callers reshape the
        result without caring which one they got.
        """
        if aoi.type == "global":
            query = global_query(dimensions, measures)
        else:
            query = admin_query(dimensions, measures, aoi.ids)
        return await self.query_services[component].execute(query)
