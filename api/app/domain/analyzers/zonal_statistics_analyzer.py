import asyncio
from abc import abstractmethod
from typing import Any, Dict, List

import dask.dataframe as dd
import newrelic.agent as nr_agent

from app.analysis.common.analysis import get_geojson
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository
from app.domain.repositories.precomputed_table import PrecomputedSource


class ZonalStatisticsAnalyzer(Analyzer):
    """Template for analyzers that serve AOIs from precomputed tables where they
    can, and compute the rest on the fly.

    Each AOI type with a precomputed table has a source, which decides which of the
    requested ids the table serves. Subclasses supply only the dataset-specific
    pieces: the ``model``, the precomputed SQL, and the per-AOI computation.
    """

    model: type

    def __init__(
        self,
        precomputed_sources: Dict[str, PrecomputedSource],
        compute_engine=None,
        input_uris: Dict[str, str] | None = None,
        otf_timeout_seconds: float = 600,
        aoi_geometry_repository: AoiGeometryRepository | None = None,
    ):
        self.precomputed_sources = precomputed_sources
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.input_uris = input_uris
        self.aoi_geometry_repository = aoi_geometry_repository
        self.otf_timeout_seconds = otf_timeout_seconds

    @nr_agent.function_trace(name="ZonalStatisticsAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        if self.input_uris is None:
            raise Exception("Input URIs must be provided for actual analysis")

        analytics_in = self.model(**analysis.metadata)
        aoi = analytics_in.aoi
        source = self.precomputed_sources.get(aoi.type)
        results = []
        otf_ids = aoi.ids
        if source is not None:
            covered_ids = await source.coverage.select_covered_ids(aoi.ids)
            precomputed_ids = [aoi_id for aoi_id in aoi.ids if aoi_id in covered_ids]
            otf_ids = [aoi_id for aoi_id in aoi.ids if aoi_id not in covered_ids]
            if precomputed_ids:
                results.append(
                    await self._run_precomputed(source, analytics_in, precomputed_ids)
                )
        if otf_ids:
            results.append(
                await self._run_on_the_fly(with_aoi_ids(analytics_in, otf_ids))
            )
        analysis.result = await asyncio.to_thread(combine_columns, results)

    async def _run_precomputed(
        self, source: PrecomputedSource, analytics_in, aoi_ids: List[str]
    ) -> Dict[str, Any]:
        data: Dict = await source.query_service.execute(
            self.build_precomputed_query(analytics_in, aoi_ids)
        )
        data["aoi_type"] = [analytics_in.aoi.type] * len(data["aoi_id"])
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
        geojsons = await get_geojson(analytics_in.aoi, self.aoi_geometry_repository)
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
    def build_precomputed_query(self, analytics_in, aoi_ids: List[str]) -> str:
        """Return the SQL that reads these AOIs' statistics from a precomputed
        table."""

    @abstractmethod
    def build_area_task(self, analytics_in):
        """Return a picklable callable ``(aoi, geojson) -> dd.DataFrame`` that
        computes the dataset's statistics for a single on-the-fly AOI."""


def with_aoi_ids(analytics_in, aoi_ids: List[str]):
    """Copy a request, keeping only these AOI ids."""
    aoi = analytics_in.aoi.model_copy(update={"ids": aoi_ids})
    return analytics_in.model_copy(update={"aoi": aoi})


def combine_columns(results: List[Dict[str, list]]) -> Dict[str, list]:
    """Join the parts of one request's result, served partly from a precomputed
    table and partly computed on the fly, into a single set of columns."""
    combined: Dict[str, list] = {column: [] for column in results[0]}
    for result in results:
        for column, values in combined.items():
            values.extend(result[column])
    return combined
