from functools import partial
from typing import Tuple

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
from app.models.land_change.natural_lands import NaturalLandsAnalyticsIn
from flox.xarray import xarray_reduce

admin_results_uri = "s3://lcl-analytics/zonal-statistics/admin-natural-lands.parquet"

NATURAL_LANDS_CLASSES = {
    2: "Natural forests",
    3: "Natural short vegetation",
    4: "Natural water",
    5: "Mangroves",
    6: "Bare",
    7: "Snow",
    8: "Wetland natural forests",
    9: "Natural peat forests",
    10: "Wetland natural short vegetation",
    11: "Natural peat short vegetation",
    12: "Cropland",
    13: "Built-up",
    14: "Non-natural tree cover",
    15: "Non-natural short vegetation",
    16: "Non-natural water",
    17: "Wetland non-natural tree cover",
    18: "Non-natural peat tree cover",
    19: "Wetland non-natural short vegetation",
    20: "Non-natural peat short vegetation",
    21: "Non-natural bare",
}


def create_gadm_natural_lands_query(gadm_id: Tuple[str, int, int]) -> str:
    # Build up the DuckDB query based on GADM ID and intersection

    from_clause = f"FROM '{admin_results_uri}'"
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

    by_clause += ", natural_lands_class"
    select_clause += ", natural_lands_class"
    group_by_clause = f"GROUP {by_clause}"
    order_by_clause = f"ORDER {by_clause}"

    # Query and make sure output names match the expected schema
    select_clause += ", SUM(area_ha) AS area_ha"
    query = f"{select_clause} {from_clause} {where_clause} {group_by_clause} {order_by_clause}"

    return query


class NaturalLandsAnalyzer(Analyzer):
    """Get the natural lands areas by class for the input AOIs"""

    def __init__(
        self,
        analysis_repository=None,
        compute_engine=None,
        dataset_repository=None,
    ):
        self.analysis_repository = analysis_repository  # NaturalLandRepository
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.dataset_repository = dataset_repository  # AWS-S3 for zarrs, etc.

    async def analyze(self, analysis: Analysis):
        natural_lands_analytics_in = NaturalLandsAnalyticsIn(**analysis.metadata)
        if natural_lands_analytics_in.aoi.type == "admin":
            analysis_partial = partial(self.analyze_admin_area)
            dd_df_futures = await self.compute_engine.gather(
                self.compute_engine.map(
                    analysis_partial, natural_lands_analytics_in.aoi.ids
                )
            )
            dfs = await self.compute_engine.gather(dd_df_futures)
            combined_results_df = await self.compute_engine.compute(dd.concat(dfs))
        else:
            aois = natural_lands_analytics_in.aoi.model_dump()
            geojsons = await get_geojson(aois)
            if aois["type"] != "feature_collection":
                aoi_list = sorted(
                    [{"type": aois["type"], "id": id} for id in aois["ids"]],
                    key=lambda aoi: aoi["id"],
                )
            else:
                aoi_list = aois["feature_collection"]["features"]
                geojsons = [geojson["geometry"] for geojson in geojsons]

            analysis_partial = partial(self.analyze_area)
            dd_df_futures = await self.compute_engine.gather(
                self.compute_engine.map(analysis_partial, aoi_list, geojsons)
            )
            dfs = await self.compute_engine.gather(dd_df_futures)
            combined_results_df = await self.compute_engine.compute(dd.concat(dfs))

            pass

        analyzed_analysis = Analysis(
            combined_results_df.to_dict(orient="list"),
            analysis.metadata,
            AnalysisStatus.saved,
        )
        await self.analysis_repository.store_analysis(
            natural_lands_analytics_in.thumbprint(), analyzed_analysis
        )

    @staticmethod
    def analyze_admin_area(id) -> dd.DataFrame:
        # GADM IDs are coming joined by '.', e.g. IDN.24.9
        gadm_id = id.split(".")

        query = create_gadm_natural_lands_query(gadm_id)

        initialize_duckdb()
        df = duckdb.query(query).df()

        df["aoi_id"] = id
        df["aoi_type"] = "admin"

        columns_to_drop = ["country"]
        if len(gadm_id) >= 2:
            columns_to_drop += ["region"]
            if len(gadm_id) == 3:
                columns_to_drop += ["subregion"]

        df = df.drop(columns=columns_to_drop, axis=1)
        ddf: dd.DataFrame = dd.from_pandas(df)
        return ddf

    @staticmethod
    def analyze_area(aoi, geojson) -> dd.DataFrame:
        natural_lands_obj_name = "s3://gfw-data-lake/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes.zarr"
        pixel_area_obj_name = "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area_ha.zarr/"
        natural_lands = read_zarr_clipped_to_geojson(
            natural_lands_obj_name, geojson
        ).band_data
        natural_lands.name = "natural_lands_class"
        pixel_area = read_zarr_clipped_to_geojson(pixel_area_obj_name, geojson)

        groupby_layers = [natural_lands]
        expected_groups = [np.arange(1, 22)]

        counts = xarray_reduce(
            pixel_area,
            *tuple(groupby_layers),
            func="sum",
            expected_groups=tuple(expected_groups),
        )

        df = (
            counts.to_dask_dataframe()
            .drop("band", axis=1)
            .drop("spatial_ref", axis=1)
            .rename(columns={"band_data": "area_ha"})
        )

        df["aoi_type"] = aoi["type"].lower()
        df["aoi_id"] = aoi["id"] if "id" in aoi else aoi["properties"]["id"]

        df["natural_lands_class"] = df.natural_lands_class.apply(
            lambda x: NATURAL_LANDS_CLASSES.get(x, "Unclassified")
        )

        df = df[df.area_ha > 0]
        return df
