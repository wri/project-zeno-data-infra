import json
import logging
import os
import traceback
from functools import partial

import dask.dataframe as dd
import duckdb
import numpy as np
import pandas as pd
import s3fs
from flox.xarray import xarray_reduce

from ..common.analysis import (
    get_geojson,
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
    pixel_area_obj_name = (
        "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area.zarr/"
    )
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
    # counts.name = "natural_lands_area"

    df = (
        counts.to_dask_dataframe()
        .drop("band", axis=1)
        .drop("spatial_ref", axis=1)
        .rename(columns={"band_data": "natural_lands_area"})
    )

    df["aoi_type"] = aoi["type"].lower()
    df["aoi_id"] = aoi["id"] if "id" in aoi else aoi["properties"]["id"]

    df["natural_lands_class"] = df.natural_lands_class.apply(
        lambda x: NATURAL_LANDS_CLASSES.get(x, "Unclassified")
    )

    df = df[df.natural_lands_area > 0]
    return df


async def get_precomputed_statistics(aoi, dask_client):
    if aoi["type"] != "admin":
        raise ValueError(
            f"No precomputed statistics available for AOI type {aoi['type']}"
        )

    # Dumbly doing this per request since the STS token expires eventually otherwise
    # According to this issue, duckdb should auto refresh the token in 1.3.0,
    # but it doesn't seem to work for us and people are reporting the same on the issue
    # https://github.com/duckdb/duckdb-aws/issues/26
    # TODO do this on lifecycle start once autorefresh works
    duckdb.query(
        """
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            PROVIDER credential_chain,
            CHAIN config
        );
    """
    )

    # Should eventually rename to this, but currently parquet file is "gadm_adm2".
    # table = "gadm_natural_lands_areas"
    table = "gadm_adm2"

    # PZB-271 just use DuckDB requester pays when this PR gets released: https://github.com/duckdb/duckdb/pull/18258
    # For now, we need to just download the file temporarily
    fs = s3fs.S3FileSystem(requester_pays=True)
    fs.get(
        f"s3://gfw-data-lake/sbtn_natural_lands/tabular/zonal_stats/gadm/{table}.parquet",
        f"/tmp/{table}.parquet",
    )

    precompute_partial = partial(get_precomputed_statistic_on_gadm_aoi, table=table)
    futures = dask_client.map(precompute_partial, aoi["ids"])
    results = await dask_client.gather(futures)
    df = pd.concat(results)

    os.remove(f"/tmp/{table}.parquet")

    return df


async def get_precomputed_statistic_on_gadm_aoi(id, table):
    # GADM IDs are coming joined by '.', e.g. IDN.24.9
    gadm_id = id.split(".")

    query = create_gadm_natural_lands_query(gadm_id, table)
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
