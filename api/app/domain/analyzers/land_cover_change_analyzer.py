from functools import partial

import dask.dataframe as dd
import duckdb
import numpy as np
from app.analysis.common.analysis import (
    get_geojson,
    initialize_duckdb,
    read_zarr_clipped_to_geojson,
)
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.land_cover_change import LandCoverChangeAnalyticsIn
from flox.xarray import xarray_reduce


class LandCoverChangeAnalyzer(Analyzer):
    """Get the total area (in hectares) of each land class transition from 2015 to 2024."""

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

    number_of_classes = len(land_cover_mapping)

    def __init__(
        self,
        analysis_repository=None,
        compute_engine=None,
        dataset_repository=None,
    ):
        self.analysis_repository = analysis_repository  # LandCoverChangeRepository
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.dataset_repository = dataset_repository  # AWS-S3 for zarrs, etc.
        self.admin_results_uri = (
            "s3://lcl-analytics/zonal-statistics/admin-land-cover-change.parquet"
        )
        self.land_cover_zarr_uri = "s3://gfw-data-lake/umd_lcl_land_cover/v2/raster/epsg-4326/zarr/umd_lcl_land_cover_2015-2024.zarr/"
        self.pixel_area_zarr_uri = "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area_ha.zarr/"

    async def analyze(self, analysis: Analysis):
        land_cover_change_analytics_in = LandCoverChangeAnalyticsIn(**analysis.metadata)
        if land_cover_change_analytics_in.aoi.type == "admin":
            gadm_ids = land_cover_change_analytics_in.aoi.ids
            combined_results_df = self.analyze_admin_areas(gadm_ids)

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
        analyzed_analysis = Analysis(
            combined_results_df.to_dict(orient="list"),
            analysis.metadata,
            AnalysisStatus.saved,
        )
        await self.analysis_repository.store_analysis(
            land_cover_change_analytics_in.thumbprint(), analyzed_analysis
        )

    def analyze_admin_areas(self, gadm_ids):
        query = f"select * from '{self.admin_results_uri}' where aoi_id in {gadm_ids} and land_cover_class_start != land_cover_class_end"
        initialize_duckdb()
        df = duckdb.query(query).df()
        df["aoi_type"] = "admin"

        return df

    @staticmethod
    def analyze_area(aoi, geojson, land_cover_zarr_uri, pixel_area_zarr_uri):
        umd_land_cover = read_zarr_clipped_to_geojson(land_cover_zarr_uri, geojson)
        pixel_area = read_zarr_clipped_to_geojson(pixel_area_zarr_uri, geojson)

        lc_data_2015 = umd_land_cover.band_data.sel(year=2015)
        lc_data_2024 = umd_land_cover.band_data.sel(year=2024)

        # Create unique identifiers for each transition using a row×column encoding:
        # row=start_class (2015), column=end_class (2024)
        # Each transition gets unique id = row*9 + column
        # e.g. class 2->5 transition = 2*9 + 5 = 23
        lc_class_change = (
            lc_data_2015 * LandCoverChangeAnalyzer.number_of_classes + lc_data_2024
        )
        lc_class_change.name = "class_change"

        land_cover_change = xarray_reduce(
            pixel_area,
            lc_class_change,
            func="sum",
            expected_groups=(np.arange(81),),
        )

        land_cover_change_df = LandCoverChangeAnalyzer._post_process_results(
            land_cover_change
        )
        land_cover_change_df["aoi_type"] = aoi["type"].lower()
        land_cover_change_df["aoi_id"] = (
            aoi["id"] if "id" in aoi else aoi["properties"]["id"]
        )

        return land_cover_change_df

    @staticmethod
    def _post_process_results(result_ds):
        land_cover_change_ddf = (
            result_ds.to_dask_dataframe()
            .drop("band", axis=1)
            .drop("spatial_ref", axis=1)
            .reset_index(drop=True)
        )

        land_cover_change_ddf[
            "land_cover_class_start"
        ] = land_cover_change_ddf.class_change.apply(
            lambda x: LandCoverChangeAnalyzer.land_cover_mapping[
                x // LandCoverChangeAnalyzer.number_of_classes
            ]
        )
        land_cover_change_ddf[
            "land_cover_class_end"
        ] = land_cover_change_ddf.class_change.apply(
            lambda x: LandCoverChangeAnalyzer.land_cover_mapping[
                x % LandCoverChangeAnalyzer.number_of_classes
            ]
        )

        land_cover_change_ddf = land_cover_change_ddf.drop(
            columns=["class_change"], axis=1
        ).rename(columns={"band_data": "change_area"})
        land_cover_change_ddf = land_cover_change_ddf[
            land_cover_change_ddf.land_cover_class_start
            != land_cover_change_ddf.land_cover_class_end
        ]

        land_cover_change_ddf["area_ha"] = land_cover_change_ddf.pop("change_area")

        return land_cover_change_ddf
