import asyncio
from abc import abstractmethod
from typing import Any, Dict

import dask.dataframe as dd
import newrelic.agent as nr_agent

from app.analysis.common.analysis import get_geojson
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis


class ZonalStatisticsAnalyzer(Analyzer):
    """Template for analyzers that serve admin AOIs from a precomputed table and
    compute all other AOI types on the fly.

    The routing and the precomputed/on-the-fly orchestration live here. Subclasses
    supply only the dataset-specific pieces: the ``model``, the admin SQL, and the
    per-AOI computation. They override those as real code, not configuration.
    """

    model: type

    def __init__(
        self,
        compute_engine=None,
        duckdb_query_service=None,
        input_uris: Dict[str, str] | None = None,
        otf_timeout_seconds: float = 600,
    ):
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.duckdb_query_service = duckdb_query_service
        self.input_uris = input_uris
        self.otf_timeout_seconds = otf_timeout_seconds

    @nr_agent.function_trace(name="ZonalStatisticsAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        if self.input_uris is None:
            raise Exception("Input URIs must be provided for actual analysis")

        analytics_in = self.model(**analysis.metadata)

        if analytics_in.aoi.type == "admin":
            analysis.result = await self._run_precomputed(analytics_in)
        else:
            analysis.result = await self._run_on_the_fly(analytics_in)

    async def _run_precomputed(self, analytics_in) -> Dict[str, Any]:
        data: Dict = await self.duckdb_query_service.execute(
            self.build_admin_query(analytics_in)
        )
        data["aoi_type"] = ["admin"] * len(data["aoi_id"])
        return data

    async def _run_on_the_fly(self, analytics_in) -> Dict[str, Any]:
        # Bound the whole on-the-fly computation so a hung/slow Dask job fails the
        # analysis (-> "failed") instead of leaving the resource pending forever.
        return await asyncio.wait_for(
            self._compute_on_the_fly(analytics_in),
            timeout=self.otf_timeout_seconds,
        )

    async def _compute_on_the_fly(self, analytics_in) -> Dict[str, Any]:
        aois = analytics_in.aoi.model_dump()
        geojsons = await get_geojson(aois)
        if aois["type"] != "feature_collection":
            aoi_list = sorted(
                [{"type": aois["type"], "id": id} for id in aois["ids"]],
                key=lambda aoi: aoi["id"],
            )
        else:
            aoi_list = aois["feature_collection"]["features"]
            geojsons = [geojson["geometry"] for geojson in geojsons]

        area_task = self.build_area_task(analytics_in)
        dd_df_futures = await self.compute_engine.gather(
            self.compute_engine.map(area_task, aoi_list, geojsons)
        )
        dfs = await self.compute_engine.gather(dd_df_futures)
        combined_results_df = await self.compute_engine.compute(dd.concat(dfs))
        return combined_results_df.to_dict(orient="list")

    @abstractmethod
    def build_admin_query(self, analytics_in) -> str:
        """Return the SQL to run against the precomputed admin table."""

    @abstractmethod
    def build_area_task(self, analytics_in):
        """Return a picklable callable ``(aoi, geojson) -> dd.DataFrame`` that
        computes the dataset's statistics for a single on-the-fly AOI."""
