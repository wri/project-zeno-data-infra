from functools import partial

import dask.dataframe as dd
import numpy as np
from app.analysis.common.analysis import (
    get_geojson,
    read_zarr_clipped_to_geojson,
)
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.grasslands import GrasslandsAnalyticsIn
from dask.dataframe import DataFrame
from xarray import DataArray


class GrasslandsAnalyzer(Analyzer):
    """Get natural/semi-natural grasslands areas for the input AOIs"""

    def __init__(
        self,
        analysis_repository=None,
        compute_engine=None,
        dataset_repository=None,
        duckdb_query_service=None,
    ):
        self.analysis_repository = analysis_repository  # GrasslandsRepository
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.dataset_repository = dataset_repository  # AWS-S3 for zarrs, etc.
        self.duckdb_query_service = duckdb_query_service

    async def analyze(self, analysis: Analysis):
        grasslands_analytics_in = GrasslandsAnalyticsIn(**analysis.metadata)
        if grasslands_analytics_in.aoi.type == "admin":
            gadm_ids = grasslands_analytics_in.aoi.ids
            results: DataFrame = await self.analyze_admin_areas(
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
            )
            dd_df_futures = await self.compute_engine.gather(
                self.compute_engine.map(analysis_partial, aoi_list, geojsons)
            )
            dfs = await self.compute_engine.gather(dd_df_futures)
            combined_results_df = await self.compute_engine.compute(dd.concat(dfs))
            results = combined_results_df.to_dict(orient="list")

        analyzed_analysis = Analysis(
            results,
            analysis.metadata,
            AnalysisStatus.saved,
        )
        await self.analysis_repository.store_analysis(
            grasslands_analytics_in.thumbprint(), analyzed_analysis
        )

    async def analyze_admin_areas(self, gadm_ids, start_year, end_year) -> DataFrame:
        query = f"select year, area_ha, aoi_id from data_source where aoi_id in {gadm_ids} and year >= {start_year} and year <= {end_year} order by aoi_id, year"
        df = await self.duckdb_query_service.execute(query)
        df["aoi_type"] = "admin"
        return df

    @staticmethod
    def analyze_area(aoi, geojson, start_year, end_year) -> dd.DataFrame:
        grasslands_obj_name = (
            "s3://gfw-data-lake/gfw_grasslands/v1/zarr/natural_grasslands_4kchunk.zarr/"
        )
        pixel_area_obj_name = "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area_ha.zarr/"
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
