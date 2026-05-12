from functools import partial
from typing import Dict, List

import newrelic.agent as nr_agent
import numpy as np
from dask.dataframe import DataFrame, concat
from xarray import DataArray

from app.analysis.common.analysis import get_geojson, read_zarr_clipped_to_geojson
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.dataset import Dataset
from app.domain.models.environment import Environment
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.models.land_change.grasslands import GrasslandsAnalyticsIn

# We want to move this into configuration, but will tolerate it being here for now.
# Note that it actually gets passed in to the constructor for easy moving later.
# Please DO NOT directly reference in constructor.
INPUT_URIS = {
    Environment.staging: {},
    Environment.production: {
        "grasslands_zarr_uri": "s3://gfw-data-lake/gfw_grasslands/v1/zarr/natural_grasslands_4kchunk.zarr/",
        str(Dataset.area_hectares): ZarrDatasetRepository.resolve_zarr_uri(
            Dataset.area_hectares, Environment.production
        ),
        "admin_results_table_uri": "s3://lcl-analytics/zonal-statistics/admin-grasslands.parquet",
    },
}


class GrasslandsAnalyzer(Analyzer):
    """Get natural/semi-natural grasslands areas for the input AOIs"""

    def __init__(
        self,
        compute_engine=None,
        duckdb_query_service=None,
        input_uris: Dict[str, str] | None = None,
    ):
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.duckdb_query_service = duckdb_query_service
        self.input_uris = input_uris

    @nr_agent.function_trace(name="GrasslandsAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        if self.input_uris is None:
            raise Exception("Input URIs must be provided for actual analysis")

        grasslands_analytics_in = GrasslandsAnalyticsIn(**analysis.metadata)

        if grasslands_analytics_in.aoi.type == "admin":
            gadm_ids: List = grasslands_analytics_in.aoi.ids
            results: Dict = await self.analyze_admin_areas(
                gadm_ids,
                grasslands_analytics_in.start_year,
                grasslands_analytics_in.end_year,
            )
        else:
            aois = grasslands_analytics_in.aoi.model_dump()
            geojsons = await get_geojson(aois)
            if aois["type"] != "feature_collection":
                aoi_list = sorted(
                    [{"type": aois["type"], "id": id} for id in aois["ids"]],
                    key=lambda aoi: aoi["id"],
                )
            else:
                aoi_list = aois["feature_collection"]["features"]
                geojsons = [geojson["geometry"] for geojson in geojsons]

            analysis_partial = partial(
                self.analyze_area,
                start_year=grasslands_analytics_in.start_year,
                end_year=grasslands_analytics_in.end_year,
                grasslands_obj_name=self.input_uris["grasslands_zarr_uri"],
                pixel_area_obj_name=self.input_uris[str(Dataset.area_hectares)],
            )
            dd_df_futures = await self.compute_engine.gather(
                self.compute_engine.map(analysis_partial, aoi_list, geojsons)
            )
            dfs = await self.compute_engine.gather(dd_df_futures)
            combined_results_df = await self.compute_engine.compute(concat(dfs))
            results = combined_results_df.to_dict(orient="list")

        analysis.result = results

    async def analyze_admin_areas(self, gadm_ids, start_year, end_year) -> Dict:
        id_str = ", ".join([f"'{aoi_id}'" for aoi_id in gadm_ids])
        query = f"select year, area_ha, aoi_id from data_source where aoi_id in ({id_str}) and year >= {start_year} and year <= {end_year} order by aoi_id, year"

        data: Dict = await self.duckdb_query_service.execute(query)
        data["aoi_type"] = "admin" * len(gadm_ids)

        return data

    @staticmethod
    def analyze_area(
        aoi, geojson, start_year, end_year, grasslands_obj_name, pixel_area_obj_name
    ) -> DataFrame:
        # Sadly, this method must be static because Dask can't serialize compute_engine
        # (a live Dask Task) in self
        grasslands: DataArray = read_zarr_clipped_to_geojson(
            grasslands_obj_name, geojson
        ).sel(year=slice(start_year, end_year))

        pixel_area = read_zarr_clipped_to_geojson(
            pixel_area_obj_name, geojson
        ).reindex_like(grasslands, method="nearest", tolerance=1e-5)

        grasslands_only = (grasslands == 2).astype(np.uint8)

        grasslands_pixel_areas = grasslands_only * pixel_area
        grasslands_areas_df = (
            grasslands_pixel_areas.sum(dim=("x", "y"))
            .to_dask_dataframe()
            .drop("spatial_ref", axis=1)
            .drop("band", axis=1)
            .rename(columns={"band_data": "area_ha"})
        )

        grasslands_areas_df["aoi_type"] = aoi["type"].lower()
        grasslands_areas_df["aoi_id"] = (
            aoi["id"] if "id" in aoi else aoi["properties"]["id"]
        )

        return grasslands_areas_df
