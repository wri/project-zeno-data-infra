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
from app.models.land_change.land_cover_composition import (
    LandCoverCompositionAnalyticsIn,
)
from flox.xarray import xarray_reduce


class LandCoverCompositionAnalyzer(Analyzer):
    """Get the total area (in hectares) of each land class composition for 2024."""

    land_cover_mapping = {
        0: "Bare and sparse vegetation",
        1: "Short vegetation",
        2: "Tree cover",
        3: "Wetland – short vegetation",
        4: "Water",
        5: "Snow/ice",
        6: "Cropland",
        7: "Built-up",
        8: "Cultivated grasslands",
    }

    def __init__(
        self,
        analysis_repository=None,
        compute_engine=None,
        dataset_repository=None,
        query_service=None,
    ):
        self.analysis_repository = analysis_repository  # LandCoverChangeRepository
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.dataset_repository = dataset_repository  # AWS-S3 for zarrs, etc.
        self.query_service = query_service
        self.admin_results_uri = "s3://lcl-analytics/zonal-statistics/admin-land-cover-composition-2024.parquet"
        self.land_cover_zarr_uri = "s3://gfw-data-lake/umd_lcl_land_cover/v2/raster/epsg-4326/zarr/umd_lcl_land_cover_2015-2024.zarr/"
        self.pixel_area_zarr_uri = "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area_ha.zarr/"

    async def analyze(self, analysis: Analysis):
        land_cover_change_analytics_in = LandCoverCompositionAnalyticsIn(
            **analysis.metadata
        )
        if land_cover_change_analytics_in.aoi.type == "admin":
            gadm_ids = land_cover_change_analytics_in.aoi.ids
            results = await self.analyze_admin_areas(gadm_ids)

        else:
            aois = land_cover_change_analytics_in.aoi.model_dump()
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
                land_cover_zarr_uri=self.land_cover_zarr_uri,
                pixel_area_zarr_uri=self.pixel_area_zarr_uri,
            )
            dd_df_futures = await self.compute_engine.gather(
                self.compute_engine.map(analysis_partial, aoi_list, geojsons)
            )
            dfs = await self.compute_engine.gather(dd_df_futures)
            combined_results_df = await self.compute_engine.compute(dd.concat(dfs))
            combined_results_df = combined_results_df[combined_results_df.area_ha > 0]
            results = combined_results_df.to_dict(orient="list")

        analyzed_analysis = Analysis(
            results,
            analysis.metadata,
            AnalysisStatus.saved,
        )
        await self.analysis_repository.store_analysis(
            land_cover_change_analytics_in.thumbprint(), analyzed_analysis
        )

    async def analyze_admin_areas(self, gadm_ids):
        id_str = (", ").join([f"'{aoi_id}'" for aoi_id in gadm_ids])
        query = f"select * from data_source where aoi_id in ({id_str}) and area_ha > 0"

        df = await self.query_service.execute(query)
        df["aoi_type"] = ["admin"] * len(df["aoi_id"])

        return df

    @staticmethod
    def analyze_area(aoi, geojson, land_cover_zarr_uri, pixel_area_zarr_uri):
        umd_land_cover = read_zarr_clipped_to_geojson(land_cover_zarr_uri, geojson)
        pixel_area = read_zarr_clipped_to_geojson(pixel_area_zarr_uri, geojson)

        lc_data_2024 = umd_land_cover.band_data.sel(year=2024)
        lc_data_2024.name = "land_cover_class"

        land_cover_composition = xarray_reduce(
            pixel_area,
            lc_data_2024,
            func="sum",
            expected_groups=(np.arange(9),),
        )

        land_cover_composition_df = LandCoverCompositionAnalyzer._post_process_results(
            land_cover_composition
        )
        land_cover_composition_df["aoi_type"] = aoi["type"].lower()
        land_cover_composition_df["aoi_id"] = (
            aoi["id"] if "id" in aoi else aoi["properties"]["id"]
        )

        return land_cover_composition_df

    @staticmethod
    def _post_process_results(result_ds):
        land_cover_composition_ddf = (
            result_ds.to_dask_dataframe()
            .drop("band", axis=1)
            .drop("year", axis=1)
            .drop("spatial_ref", axis=1)
            .reset_index(drop=True)
        )
        land_cover_composition_ddf[
            "land_cover_class"
        ] = land_cover_composition_ddf.land_cover_class.apply(
            lambda x: LandCoverCompositionAnalyzer.land_cover_mapping[x]
        )
        land_cover_composition_ddf["area_ha"] = land_cover_composition_ddf.pop(
            "band_data"
        )

        return land_cover_composition_ddf
