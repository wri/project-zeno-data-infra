import json
import logging
import traceback
from functools import partial

import dask.dataframe as dd
import duckdb
import numpy as np
import pandas as pd
from flox.xarray import xarray_reduce

from ..common.analysis import (
    get_geojson,
    initialize_duckdb,
    read_zarr_clipped_to_geojson,
)
from .query import create_gadm_natural_lands_query

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


async def zonal_statistics_on_aois(aois, dask_client):
    geojsons = await get_geojson(aois)
    if aois["type"] != "feature_collection":
        aois = sorted(
            [{"type": aois["type"], "id": id} for id in aois["ids"]],
            key=lambda aoi: aoi["id"],
        )
    else:
        aois = aois["feature_collection"]["features"]
        geojsons = [geojson["geometry"] for geojson in geojsons]

    precompute_partial = partial(zonal_statistics)
    dd_df_futures = await dask_client.gather(
        dask_client.map(precompute_partial, aois, geojsons)
    )
    dfs = await dask_client.gather(dd_df_futures)
    df = await dask_client.compute(dd.concat(dfs))

    return df


async def zonal_statistics(aoi, geojson):
    natural_lands_obj_name = (
        "s3://gfw-data-lake/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes.zarr"
    )
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


async def get_precomputed_statistics(aoi, dask_client):
    if aoi["type"] != "admin":
        raise ValueError(
            f"No precomputed statistics available for AOI type {aoi['type']}"
        )

    table = "s3://lcl-analytics/zonal-statistics/admin-natural-lands.parquet"

    precompute_partial = partial(get_precomputed_statistic_on_gadm_aoi, table=table)
    futures = dask_client.map(precompute_partial, aoi["ids"])
    results = await dask_client.gather(futures)
    df = pd.concat(results)

    return df


async def get_precomputed_statistic_on_gadm_aoi(id, table):
    # GADM IDs are coming joined by '.', e.g. IDN.24.9
    gadm_id = id.split(".")

    query = create_gadm_natural_lands_query(gadm_id, table)

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

    return df


async def do_analytics(file_path, dask_client):
    try:
        # Read and parse JSON file
        metadata = file_path / "metadata.json"
        json_content = metadata.read_text()
        metadata_content = json.loads(json_content)  # Convert JSON to Python object
        aois = metadata_content["aoi"]

        if aois["type"] == "admin":
            df = await get_precomputed_statistics(aois, dask_client)
        else:
            df = await zonal_statistics_on_aois(aois, dask_client)

        results_dict = df.to_dict(orient="list")

        data = file_path / "data.json"
        data.write_text(json.dumps(results_dict))
    except Exception as e:
        logging.error(
            {
                "event": "natural_lands_analytics_processing_failure",
                "severity": "high",  # Helps with alerting
                "metadata": metadata_content,
                "error_type": e.__class__.__name__,  # e.g., "ValueError", "ConnectionError"
                "error_details": str(e),
                "stack_trace": traceback.format_exc(),
            }
        )
