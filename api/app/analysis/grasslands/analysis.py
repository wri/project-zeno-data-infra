import json
import logging
import traceback
from functools import partial

import dask.dataframe as dd
import duckdb
import numpy as np
import pandas as pd

from ..common.analysis import (
    get_geojson,
    read_zarr_clipped_to_geojson,
)
from .query import create_gadm_grasslands_query


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
    grassland_areas_df = await dask_client.compute(dd.concat(dfs))

    return grassland_areas_df


async def zonal_statistics(aoi, geojson):
    grasslands_obj_name = (
        "s3://gfw-data-lake/gfw_grasslands/v1/zarr/natural_grasslands_2kchunk.zarr"
    )
    pixel_area_obj_name = (
        "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area.zarr/"
    )
    grasslands = read_zarr_clipped_to_geojson(grasslands_obj_name, geojson)
    pixel_area = read_zarr_clipped_to_geojson(pixel_area_obj_name, geojson)
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


async def get_precomputed_statistics(aoi, dask_client):
    if aoi["type"] != "admin":
        raise ValueError(
            f"No precomputed statistics available for AOI type {aoi['type']}"
        )

    parquet_file_uri = "s3://lcl-analytics/zonal-statistics/admin-grasslands.parquet"
    precompute_partial = partial(
        get_precomputed_statistic_on_gadm_aoi, parquet_file=parquet_file_uri
    )
    futures = dask_client.map(precompute_partial, aoi["ids"])
    results = await dask_client.gather(futures)
    yearly_grassland_areas_df = pd.concat(results)

    return yearly_grassland_areas_df


async def get_precomputed_statistic_on_gadm_aoi(id, parquet_file):
    # GADM IDs are coming joined by '.', e.g. IDN.24.9
    gadm_id = id.split(".")

    query = create_gadm_grasslands_query(gadm_id, parquet_file)

    duckdb.query(
        """
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            PROVIDER credential_chain,
            CHAIN config
        );
    """
    )
    grasslands_df = duckdb.query(query).df()

    grasslands_df["aoi_id"] = id
    grasslands_df["aoi_type"] = "admin"

    columns_to_drop = ["country"]
    if len(gadm_id) > 1:
        columns_to_drop += ["region"]
    if len(gadm_id) > 2:
        columns_to_drop += ["subregion"]

    grasslands_df = grasslands_df.drop(columns=columns_to_drop, axis=1)

    return grasslands_df


async def do_analytics(file_path, dask_client):
    try:
        # Read and parse JSON file
        metadata = file_path / "metadata.json"
        json_content = metadata.read_text()
        metadata_content = json.loads(json_content)  # Convert JSON to Python object
        aois = metadata_content["aoi"]

        if aois["type"] == "admin":
            grasslands_df = await get_precomputed_statistics(aois, dask_client)
        else:
            grasslands_df = await zonal_statistics_on_aois(aois, dask_client)

        if metadata_content["start_year"] is not None:
            grasslands_df = grasslands_df[
                grasslands_df.year >= int(metadata_content["start_year"])
            ]
        if metadata_content["end_year"] is not None:
            grasslands_df = grasslands_df[
                grasslands_df.year <= int(metadata_content["end_year"])
            ]
        grasslands_dict = grasslands_df.to_dict(orient="list")

        data = file_path / "data.json"
        data.write_text(json.dumps(grasslands_dict))
    except Exception as e:
        logging.error(
            {
                "event": "grasslands_analytics_processing_failure",
                "severity": "high",  # Helps with alerting
                "metadata": metadata_content,
                "error_type": e.__class__.__name__,  # e.g., "ValueError", "ConnectionError"
                "error_details": str(e),
                "stack_trace": traceback.format_exc(),
            }
        )
