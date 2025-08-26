import json
import logging
import os
import traceback
from functools import partial
from typing import Optional

import dask.dataframe as dd
import duckdb
import numpy as np
import pandas as pd
import s3fs
import xarray as xr
from dask.dataframe import DataFrame as DaskDataFrame
from flox.xarray import xarray_reduce

from ..common.analysis import (
    JULIAN_DATE_2021,
    get_geojson,
    read_zarr_clipped_to_geojson,
)
from .query import create_gadm_dist_query

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


async def zonal_statistics_on_aois(aois, dask_client, intersection=None):
    geojsons = await get_geojson(aois)

    if aois["type"] != "feature_collection":
        aois = sorted(
            [{"type": aois["type"], "id": id} for id in aois["ids"]],
            key=lambda aoi: aoi["id"],
        )
    else:
        aois = aois["feature_collection"]["features"]
        geojsons = [geojson["geometry"] for geojson in geojsons]

    precompute_partial = partial(zonal_statistics, intersection=intersection)
    dd_df_futures = await dask_client.gather(
        dask_client.map(precompute_partial, aois, geojsons)
    )
    dfs = await dask_client.gather(dd_df_futures)
    alerts_df = await dask_client.compute(dd.concat(dfs))

    return alerts_df


async def zonal_statistics(
    aoi, geojson, intersection: Optional[str] = None
) -> DaskDataFrame:
    dist_obj_name = "s3://gfw-data-lake/umd_glad_dist_alerts/v20250510/raster/epsg-4326/zarr/date_conf.zarr"
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
    alerts_area.name = "value"

    alerts_df: DaskDataFrame = (
        alerts_area.to_dask_dataframe()
        .drop("band", axis=1)
        .drop("spatial_ref", axis=1)
        .reset_index(drop=True)
    )
    alerts_df.confidence = alerts_df.confidence.map(
        {2: "low", 3: "high"}, meta=("confidence", "uint8")
    )
    alerts_df.alert_date = dd.to_datetime(
        alerts_df.alert_date + JULIAN_DATE_2021, origin="julian", unit="D"
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

    alerts_df = alerts_df[alerts_df.value > 0]
    return alerts_df


async def get_precomputed_statistics(aoi, intersection: Optional[str], dask_client):
    if aoi["type"] != "admin" or intersection not in [
        None,
        "natural_lands",
        "driver",
        "grasslands",
        "land_cover",
    ]:
        raise ValueError(
            f"No precomputed statistics available for AOI type {aoi['type']} and intersection {intersection}"
        )

    table = get_precomputed_table(aoi["type"], intersection)

    # PZB-271 just use DuckDB requester pays when this PR gets released: https://github.com/duckdb/duckdb/pull/18258
    # For now, we need to just download the file temporarily
    fs = s3fs.S3FileSystem(requester_pays=True)
    fs.get(
        f"s3://gfw-data-lake/umd_glad_dist_alerts/parquet2/{table}.parquet",
        f"/tmp/{table}.parquet",
    )

    precompute_partial = partial(
        get_precomputed_statistic_on_gadm_aoi, table=table, intersection=intersection
    )
    futures = dask_client.map(precompute_partial, aoi["ids"])
    results = await dask_client.gather(futures)
    alerts_df = pd.concat(results)

    os.remove(f"/tmp/{table}.parquet")

    return alerts_df


def get_precomputed_table(aoi_type: str, intersection: Optional[str]) -> str:
    if aoi_type == "admin":
        # Each intersection will be in a different parquet file
        if intersection is None:
            table = "gadm_dist_alerts"
        elif intersection == "driver":
            table = "gadm_dist_alerts_by_driver"
        elif intersection == "natural_lands":
            table = "gadm_dist_alerts_by_natural_lands"
        elif intersection == "grasslands":
            table = "gadm_dist_alerts_by_grasslands"
        elif intersection == "land_cover":
            table = "gadm_dist_alerts_by_land_cover"
        else:
            raise ValueError(f"No way to calculate intersection {intersection}")
    else:
        raise ValueError(f"No way to calculate aoi type {aoi_type}")

    return table


async def get_precomputed_statistic_on_gadm_aoi(id, table, intersection):
    # GADM IDs are coming joined by '.', e.g. IDN.24.9
    gadm_id = id.split(".")

    query = create_gadm_dist_query(gadm_id, table, intersection)
    alerts_df = duckdb.query(query).df()

    alerts_df["aoi_id"] = id
    alerts_df["aoi_type"] = "admin"

    return alerts_df


async def do_analytics(file_path, dask_client):
    try:
        # Read and parse JSON file
        metadata = file_path / "metadata.json"
        json_content = metadata.read_text()
        metadata_content = json.loads(json_content)  # Convert JSON to Python object
        aois = metadata_content["aoi"]

        # for now we only support one intersection, as enforced by the route
        intersections = metadata_content["intersections"]
        if intersections:
            intersection = intersections[0]
        else:
            intersection = None

        if aois["type"] == "admin":
            alerts_df = await get_precomputed_statistics(
                aois, intersection, dask_client
            )
        else:
            alerts_df = await zonal_statistics_on_aois(aois, dask_client, intersection)

        if metadata_content["start_date"] is not None:
            alerts_df = alerts_df[
                alerts_df.alert_date >= metadata_content["start_date"]
            ]
        if metadata_content["end_date"] is not None:
            alerts_df = alerts_df[alerts_df.alert_date <= metadata_content["end_date"]]
        alerts_dict = alerts_df.to_dict(orient="list")

        data = file_path / "data.json"
        data.write_text(json.dumps(alerts_dict))
    except Exception as e:
        logging.error(
            {
                "event": "dist_alerts_analytics_processing_failure",
                "severity": "high",  # Helps with alerting
                "metadata": metadata_content,
                "error_type": e.__class__.__name__,  # e.g., "ValueError", "ConnectionError"
                "error_details": str(e),
                "stack_trace": traceback.format_exc(),
            }
        )
