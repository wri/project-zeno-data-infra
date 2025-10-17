from functools import partial
from typing import Any, Dict, List, Optional

import dask.dataframe as dd
import numpy as np
import xarray as xr
from dask.dataframe import DataFrame as DaskDataFrame
from flox.xarray import xarray_reduce

from app.analysis.common.analysis import (
    JULIAN_DATE_2021,
    get_geojson,
    read_zarr_clipped_to_geojson,
)
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.models.land_change.dist_alerts import DistAlertsAnalyticsIn

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

DIST_DRIVERS = {
    1: "Wildfire",
    2: "Flooding",
    3: "Crop management",
    4: "Potential conversion",
    5: "Unclassified",
}

LAND_COVER_MAPPING = {
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


class DistAlertsAnalyzer(Analyzer):
    def __init__(
        self,
        analysis_repository=None,
        compute_engine=None,
        dataset_repository=None,
        duckdb_query_service=None,
    ):
        self.analysis_repository = analysis_repository
        self.compute_engine = compute_engine
        self.dataset_repository = dataset_repository
        self.duckdb_query_service = duckdb_query_service

    async def analyze(self, analysis: Analysis):
        dist_analytics_in = DistAlertsAnalyticsIn(**analysis.metadata)

        # for now we only support one intersection, as enforced by the route
        if dist_analytics_in.intersections:
            intersection = dist_analytics_in.intersections[0]
        else:
            intersection = None

        version = analysis.metadata["_version"]
        if dist_analytics_in.aoi.type == "admin":
            alerts_dict = await self.analyze_admin_areas(
                dist_analytics_in.aoi.ids,
                version,
                dist_analytics_in.start_date,
                dist_analytics_in.end_date,
                intersection,
            )
        else:
            alerts_df = await self.zonal_statistics_on_aois(
                dist_analytics_in.model_dump()["aoi"],
                self.compute_engine,
                version,
                intersection,
            )

            if dist_analytics_in.start_date is not None:
                alerts_df = alerts_df[
                    alerts_df.dist_alert_date >= dist_analytics_in.start_date
                ]
            if dist_analytics_in.end_date is not None:
                alerts_df = alerts_df[
                    alerts_df.dist_alert_date <= dist_analytics_in.end_date
                ]
            alerts_dict = alerts_df.to_dict(orient="list")

        return alerts_dict

    async def analyze_admin_areas(
        self,
        aoi_ids: List[str],
        version: str,
        start_date: str,
        end_date: str,
        intersection: Optional[str] = None,
    ) -> Dict[str, Any]:
        if intersection is None:
            table = "admin-dist-alerts"
        elif intersection == "driver":
            table = "admin-dist-alerts-by-driver"
            intersection_col = "driver"
        elif intersection == "natural_lands":
            table = "admin-dist-alerts-by-natural-land-class"
            intersection_col = "natural_land_class"
        elif intersection == "grasslands":
            table = "admin-dist-alerts-by-grassland-class"
            intersection_col = "grasslands"
        elif intersection == "land_cover":
            table = "admin-dist-alerts-by-land-cover-class"
            intersection_col = "land_cover"
        else:
            raise ValueError(f"No way to calculate intersection {intersection}")

        table_uri = (
            f"s3://lcl-analytics/zonal-statistics/dist-alerts/{version}/{table}.parquet"
        )

        id_str = (", ").join([f"'{aoi_id}'" for aoi_id in aoi_ids])

        from_clause = f"FROM '{table_uri}'"
        select_clause = "SELECT aoi_id"
        where_clause = f"WHERE aoi_id in ({id_str}) AND dist_alert_date >= '{start_date}' AND dist_alert_date <= '{end_date}'"
        by_clause = "BY aoi_id, dist_alert_date, dist_alert_confidence"

        # Includes an intersection, so group by the appropriate column
        if intersection:
            select_clause += f", {intersection_col}"
            by_clause += f", {intersection_col}"

        by_clause += ", dist_alert_date, dist_alert_confidence"
        order_by_clause = f"ORDER {by_clause}"

        # Query and make sure output names match the expected schema
        select_clause += ", STRFTIME(dist_alert_date, '%Y-%m-%d') AS dist_alert_date, dist_alert_confidence, area_ha"
        query = f"{select_clause} {from_clause} {where_clause} {order_by_clause}"

        data: Dict = await self.duckdb_query_service.execute(query)
        data["aoi_type"] = "admin" * len(aoi_ids)

        return data

    async def zonal_statistics_on_aois(
        self, aois, dask_client, version, intersection=None
    ):
        geojsons = await get_geojson(aois)

        if aois["type"] != "feature_collection":
            aois = sorted(
                [{"type": aois["type"], "id": id} for id in aois["ids"]],
                key=lambda aoi: aoi["id"],
            )
        else:
            aois = aois["feature_collection"]["features"]
            geojsons = [geojson["geometry"] for geojson in geojsons]

        precompute_partial = partial(
            self.zonal_statistics, version=version, intersection=intersection
        )
        futures = dask_client.map(precompute_partial, aois, geojsons)
        dd_df_futures = await dask_client.gather(futures)
        dfs = await dask_client.gather(dd_df_futures)
        alerts_df = await dask_client.compute(dd.concat(dfs))

        return alerts_df

    @staticmethod
    async def zonal_statistics(
        aoi, geojson, version: Optional[str] = None, intersection: Optional[str] = None
    ) -> DaskDataFrame:
        dist_obj_name = (
            f"s3://lcl-analytics/zarr/dist-alerts/{version}/umd_glad_dist_alerts.zarr"
        )
        dist_alerts = read_zarr_clipped_to_geojson(dist_obj_name, geojson)

        pixel_area_uri = "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area_ha.zarr"
        pixel_area = read_zarr_clipped_to_geojson(
            pixel_area_uri, geojson
        ).band_data.reindex_like(dist_alerts, method="nearest", tolerance=1e-5)

        groupby_layers = [dist_alerts.alert_date, dist_alerts.confidence]
        expected_groups = [np.arange(731, 2000), [1, 2, 3]]
        if intersection == "natural_lands":
            natural_lands = read_zarr_clipped_to_geojson(
                "s3://gfw-data-lake/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes_clipped_to_dist.zarr",
                geojson,
            ).band_data
            natural_lands.name = "natural_lands_class"

            groupby_layers.append(natural_lands)
            expected_groups.append(np.arange(22))
        elif intersection == "driver":
            # Re-index to DIST alerts to avoid floating point precision issues
            # when aligning the datasets
            # See https://github.com/pydata/xarray/issues/2217
            dist_drivers = read_zarr_clipped_to_geojson(
                "s3://gfw-data-lake/umd_glad_dist_alerts_driver/zarr/umd_dist_alerts_drivers.zarr",
                geojson,
            ).band_data.reindex_like(dist_alerts, method="nearest", tolerance=1e-5)
            dist_drivers.name = "driver"

            groupby_layers.append(dist_drivers)
            expected_groups.append(np.arange(5))
        elif intersection == "grasslands":
            grasslands = read_zarr_clipped_to_geojson(
                "s3://gfw-data-lake/gfw_grasslands/v1/zarr/natural_grasslands_4kchunk.zarr/",
                geojson,
            ).band_data.reindex_like(dist_alerts, method="nearest", tolerance=1e-5)

            # Only use a single year of grasslands when used as a contextual layer.
            grasslands_year2022 = grasslands.sel(year=2022)
            grasslands_only = (grasslands_year2022 == 2).astype(np.uint8)
            grasslands_only.name = "grasslands"

            groupby_layers.append(grasslands_only)
            expected_groups.append([0, 1])
        elif intersection == "land_cover":
            land_cover = read_zarr_clipped_to_geojson(
                "s3://gfw-data-lake/umd_lcl_land_cover/v2/raster/epsg-4326/zarr/umd_lcl_land_cover_2015-2024.zarr",
                geojson,
            ).band_data.reindex_like(dist_alerts, method="nearest", tolerance=1e-5)

            # Only use a single year of land_cover when used as a contextual layer.
            land_cover_year2024 = land_cover.sel(year=2024)
            land_cover_year2024.name = "land_cover_class"

            groupby_layers.append(land_cover_year2024)
            expected_groups.append(np.arange(8))

        alerts_area: xr.DataArray = xarray_reduce(
            pixel_area,
            *tuple(groupby_layers),
            func="sum",
            expected_groups=tuple(expected_groups),
        )
        alerts_area.name = "area_ha"

        alerts_df: DaskDataFrame = (
            alerts_area.to_dask_dataframe()
            .drop("band", axis=1)
            .drop("spatial_ref", axis=1)
            .reset_index(drop=True)
        )
        alerts_df = alerts_df.rename(
            columns={
                "confidence": "dist_alert_confidence",
                "alert_date": "dist_alert_date",
            }
        )
        alerts_df.dist_alert_confidence = alerts_df.dist_alert_confidence.map(
            {2: "low", 3: "high"}, meta=("dist_alert_confidence", "uint8")
        )
        alerts_df.dist_alert_date = dd.to_datetime(
            alerts_df.dist_alert_date + JULIAN_DATE_2021, origin="julian", unit="D"
        ).dt.strftime("%Y-%m-%d")

        alerts_df["aoi_type"] = aoi["type"].lower()
        alerts_df["aoi_id"] = aoi["id"] if "id" in aoi else aoi["properties"]["id"]

        if intersection == "natural_lands":
            alerts_df["natural_lands_category"] = alerts_df.natural_lands_class.apply(
                lambda x: "natural" if 1 < x < 12 else "non-natural"
            )
            alerts_df["natural_land_class"] = alerts_df.natural_lands_class.apply(
                lambda x: NATURAL_LANDS_CLASSES.get(x, "Unclassified")
            )
        elif intersection == "driver":
            alerts_df.driver = alerts_df.driver.apply(
                lambda x: DIST_DRIVERS.get(x, "Unclassified")
            )
        elif intersection == "grasslands":
            alerts_df["grasslands"] = alerts_df["grasslands"].apply(
                (lambda x: "grasslands" if x == 1 else "non-grasslands"),
                meta=("grasslands", "uint8"),
            )
            alerts_df = alerts_df.drop("year", axis=1).reset_index(drop=True)
        elif intersection == "land_cover":
            alerts_df["land_cover_class"] = alerts_df["land_cover_class"].apply(
                (lambda x: LAND_COVER_MAPPING.get(x, "Unclassified"))
            )
            alerts_df = alerts_df.drop("year", axis=1).reset_index(drop=True)

        alerts_df = alerts_df[alerts_df.area_ha > 0]
        return alerts_df
