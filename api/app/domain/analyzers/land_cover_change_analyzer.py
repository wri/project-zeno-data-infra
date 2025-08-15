from abc import abstractmethod

import pandas as pd
import duckdb
import numpy as np
from flox.xarray import xarray_reduce

from functools import partial
import dask.dataframe as dd

from app.analysis.common.analysis import read_zarr_clipped_to_geojson, get_geojson
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.land_cover import LandCoverChangeAnalyticsIn


class LandCoverChangeAnalyzer(Analyzer):
    def __init__(
        self,
        analysis_repository=None,
        compute_engine=None,
        dataset_repository=None,
    ):
        self.analysis_repository = analysis_repository  # LandCoverChangeRepository
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.dataset_repository = dataset_repository  # AWS-S3 for zarrs, etc.
        self.results_uri = "s3://gfw-data-lake/umd_lcl_land_cover/v2/tabular/statistics/admin_land_cover_change.parquet"
        self.land_cover_zarr_uri = "s3://gfw-data-lake/umd_lcl_land_cover/v2/raster/epsg-4326/zarr/umd_lcl_land_cover.zarr/"
        self.pixel_area_zarr_uri = "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area.zarr/"
        self.land_cover_mapping = {
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

    async def analyze(self, analysis: Analysis):
        land_cover_change_analytics_in = LandCoverChangeAnalyticsIn(**analysis.metadata)
        if land_cover_change_analytics_in.aoi.type == "admin":
            gadm_results = []
            gadm_ids = land_cover_change_analytics_in.aoi.ids
            for gadm_id in gadm_ids:
                results = self.analyze_admin_area(gadm_id)
                results["aoi_id"] = gadm_id
                results["aoi_type"] = "admin"
                results = results[
                    results.land_cover_class_start != results.land_cover_class_end
                ]  # Filter out no change
                gadm_results.append(results)
            combined_results = pd.concat(gadm_results).to_dict(orient="list")

        else:
            aois = land_cover_change_analytics_in.aoi.model_dump()
            geojsons = await get_geojson(aois)
            if aois["type"] != "feature_collection":
                aois = sorted(
                    [{"type": aois["type"], "id": id} for id in aois["ids"]],
                    key=lambda aoi: aoi["id"],
                )
            else:
                aois = aois["feature_collection"]["features"]
                geojsons = [geojson["geometry"] for geojson in geojsons]

            analysis_partial = partial(
                self.analyze_area,
                land_cover_zarr_uri=self.land_cover_zarr_uri,
                pixel_area_zarr_uri=self.pixel_area_zarr_uri,
                land_cover_mapping=self.land_cover_mapping,
            )
            dd_df_futures = await self.compute_engine.gather(
                self.compute_engine.map(analysis_partial, aois, geojsons)
            )
            dfs = await self.compute_engine.gather(dd_df_futures)
            combined_results_df = await self.compute_engine.compute(dd.concat(dfs))
            combined_results = combined_results_df.to_dict(orient="list")

        analyzed_analysis = Analysis(
            combined_results, analysis.metadata, AnalysisStatus.saved
        )
        await self.analysis_repository.store_analysis(
            land_cover_change_analytics_in.thumbprint(), analyzed_analysis
        )

    def analyze_admin_area(self, gadm_id):
        gadm_id = gadm_id.split(".")
        query = self._build_query(gadm_id)
        df = duckdb.query(query).df()

        columns_to_drop = ["country"]
        if len(gadm_id) >= 2:
            columns_to_drop += ["region"]
        if len(gadm_id) == 3:
            columns_to_drop += ["subregion"]

        df = df.drop(columns=columns_to_drop, axis=1)

        return df

    @staticmethod
    def analyze_area(
        aoi, geojson, land_cover_zarr_uri, pixel_area_zarr_uri, land_cover_mapping
    ):
        umd_land_cover = read_zarr_clipped_to_geojson(land_cover_zarr_uri, geojson)
        pixel_area = read_zarr_clipped_to_geojson(pixel_area_zarr_uri, geojson)

        umd_land_cover = umd_land_cover.assign_coords(
            year=np.arange(2015, 2025)
        ).rename_vars({"2015": "lc_class"})

        lc_data_2015 = umd_land_cover.sel(year=2015).lc_class
        lc_data_2024 = umd_land_cover.sel(year=2024).lc_class

        lc_class_change = lc_data_2015 * 9 + lc_data_2024
        lc_class_change.name = "class_change"

        land_cover_change = xarray_reduce(
            pixel_area,
            lc_class_change,
            func="sum",
            expected_groups=(np.arange(81),),
        )

        land_cover_change_df = LandCoverChangeAnalyzer._post_process_results(
            land_cover_change, land_cover_mapping
        )
        land_cover_change_df["aoi_type"] = aoi["type"].lower()
        land_cover_change_df["aoi_id"] = (
            aoi["id"] if "id" in aoi else aoi["properties"]["id"]
        )

        return land_cover_change_df

    @staticmethod
    def _post_process_results(result_ds, land_cover_mapping):
        land_cover_change_ddf = (
            result_ds.to_dask_dataframe()
            .drop("band", axis=1)
            .drop("spatial_ref", axis=1)
            .reset_index(drop=True)
        )

        land_cover_change_ddf["land_cover_class_start"] = (
            land_cover_change_ddf.class_change.apply(
                lambda x: land_cover_mapping[x // 9]
            )
        )
        land_cover_change_ddf["land_cover_class_end"] = (
            land_cover_change_ddf.class_change.apply(
                lambda x: land_cover_mapping[x % 9]
            )
        )

        land_cover_change_ddf = land_cover_change_ddf.drop(
            columns=["class_change"], axis=1
        ).rename(columns={"band_data": "change_area"})
        land_cover_change_ddf = land_cover_change_ddf[
            land_cover_change_ddf.land_cover_class_start
            != land_cover_change_ddf.land_cover_class_end
        ]

        return land_cover_change_ddf

    def _build_query(
        self,
        gadm_id,
    ):
        """
        Build a query to get land cover change statistics for the given GADM IDs.
        """
        from_clause = f"FROM '{self.results_file}'"
        select_clause = "SELECT country"
        where_clause = f"WHERE country = '{gadm_id[0]}'"
        by_clause = "BY country"

        # Includes region, so add relevant filters, selects and group bys
        if len(gadm_id) > 1:
            select_clause += ", region"
            where_clause += f" AND region = {gadm_id[1]}"
            by_clause += ", region"

        # Includes subregion, so add relevant filters, selects and group bys
        if len(gadm_id) > 2:
            select_clause += ", subregion"
            where_clause += f" AND subregion = {gadm_id[2]}"
            by_clause += ", subregion"

        by_clause += ", land_cover_class_start, land_cover_class_end"
        select_clause += ", land_cover_class_start, land_cover_class_end"
        group_by_clause = f"GROUP {by_clause}"
        order_by_clause = f"ORDER {by_clause}"

        # Query and make sure output names match the expected schema (?)
        select_clause += ", SUM(area) AS change_area"
        query = f"{select_clause} {from_clause} {where_clause} {group_by_clause} {order_by_clause}"

        return query
