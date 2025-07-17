import json
import os
from functools import partial

import duckdb
import numpy as np
import pandas as pd
import s3fs
from flox.xarray import xarray_reduce

from ..common.analysis import (
    JULIAN_DATE_2021,
    get_geojson,
    read_zarr_clipped_to_geojson,
)
from .query import create_gadm_dist_query

NATURAL_LANDS_CLASSES = {
    2: "Forest",
    3: "Short vegetation",
    4: "Water",
    5: "Mangroves",
    6: "Bare",
    7: "Snow/Ice",
    8: "Wetland forest",
    9: "Peat forest",
    10: "Wetland short vegetation",
    11: "Peat short vegetation",
    12: "Cropland",
    13: "Built-up",
    14: "Tree cover",
    15: "Short vegetation",
    16: "Water",
    17: "Wetland tree cover",
    18: "Peat tree cover",
    19: "Wetland short vegetation",
    20: "Peat short vegetation",
    21: "Bare",
}
DIST_DRIVERS = {
    1: "Wildfire",
    2: "Flooding",
    3: "Crop management",
    4: "Potential conversion",
    5: "Unclassified",
}


async def zonal_statistics(geojson, aoi, intersection=None):
    dist_obj_name = "s3://gfw-data-lake/umd_glad_dist_alerts/v20250510/raster/epsg-4326/zarr/date_conf.zarr"
    dist_alerts = read_zarr_clipped_to_geojson(dist_obj_name, geojson)

    groupby_layers = [dist_alerts.alert_date, dist_alerts.confidence]
    expected_groups = [np.arange(731, 1590), [1, 2, 3]]
    if intersection == "natural_lands":
        natural_lands = read_zarr_clipped_to_geojson(
            "s3://gfw-data-lake/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes_clipped_to_dist.zarr",
            geojson,
        ).band_data
        natural_lands.name = "natural_land_class"

        groupby_layers.append(natural_lands)
        expected_groups.append(np.arange(22))
    elif intersection == "driver":
        dist_drivers = read_zarr_clipped_to_geojson(
            "s3://gfw-data-lake/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes_clipped_to_dist.zarr",
            geojson,
        )
        dist_drivers.name = "ldacs_driver"

        groupby_layers.append(dist_drivers)
        expected_groups.append(np.arange(5))

    alerts_count = xarray_reduce(
        dist_alerts.alert_date,
        *tuple(groupby_layers),
        func="count",
        expected_groups=tuple(expected_groups),
    ).compute()
    alerts_count.name = "value"

    alerts_df = (
        alerts_count.to_dataframe()
        .drop("band", axis=1)
        .drop("spatial_ref", axis=1)
        .reset_index()
    )
    alerts_df.confidence = alerts_df.confidence.map({2: "low", 3: "high"})
    alerts_df.alert_date = pd.to_datetime(
        alerts_df.alert_date + JULIAN_DATE_2021, origin="julian", unit="D"
    ).dt.strftime("%Y-%m-%d")

    if "ids" in aoi:
        alerts_df[aoi["type"]] = aoi["ids"][0]

    if intersection == "natural_lands":
        alerts_df.natural_land_class = alerts_df.natural_land_class.apply(
            lambda x: NATURAL_LANDS_CLASSES.get(x, "Unclassified")
        )
    elif intersection == "driver":
        alerts_df.ldacs_driver = alerts_df.ldacs_driver.apply(
            lambda x: DIST_DRIVERS.get(x, "Unclassified")
        )

    alerts_df = alerts_df[alerts_df.value > 0]
    return alerts_df


async def get_precomputed_statistics(aoi, intersections, dask_client):
    if aoi["type"] != "admin" or (
        intersections and (intersections[0] not in ["natural_lands", "driver"])
    ):
        raise ValueError(
            f"No precomputed statistics available for AOI type {aoi['type']} and intersection {intersections}"
        )

    table = get_precomputed_table(aoi["type"], intersections)

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

    # PZB-271 just use DuckDB requester pays when this PR gets released: https://github.com/duckdb/duckdb/pull/18258
    # For now, we need to just download the file temporarily
    fs = s3fs.S3FileSystem(requester_pays=True)
    fs.get(
        f"s3://gfw-data-lake/umd_glad_dist_alerts/parquet/{table}.parquet",
        f"/tmp/{table}.parquet",
    )

    precompute_partial = partial(
        get_precomputed_statistic_on_gadm_aoi, table=table, intersections=intersections
    )
    futures = dask_client.map(precompute_partial, aoi["ids"])
    results = await dask_client.gather(futures)
    alerts_df = pd.concat(results)

    os.remove(f"/tmp/{table}.parquet")

    return alerts_df


def get_precomputed_table(aoi_type, intersections):
    if aoi_type == "admin":
        # Each intersection will be in a different parquet file
        if not intersections:
            table = "gadm_dist_alerts"
        elif intersections[0] == "driver":
            table = "gadm_dist_alerts_by_driver"
        elif intersections[0] == "natural_lands":
            table = "gadm_dist_alerts_by_natural_lands"
        else:
            raise ValueError(f"No way to calculate intersection {intersections[0]}")
    else:
        raise ValueError(f"No way to calculate aoi type {aoi_type}")

    return table


async def get_precomputed_statistic_on_gadm_aoi(id, table, intersections):
    # GADM IDs are coming joined by '.', e.g. IDN.24.9
    gadm_id = id.split(".")

    query = create_gadm_dist_query(gadm_id, table, intersections)
    alerts_df = duckdb.query(query).df()
    return alerts_df


async def do_analytics(file_path, dask_client):
    # Read and parse JSON file
    metadata = file_path / "metadata.json"
    json_content = metadata.read_text()
    metadata_content = json.loads(json_content)  # Convert JSON to Python object
    aoi = metadata_content["aoi"]
    if aoi["type"] == "admin":
        alerts_df = await get_precomputed_statistics(
            aoi, metadata_content["intersections"], dask_client
        )
    else:
        geojson = await get_geojson(aoi)

        if metadata_content["intersections"]:
            intersection = metadata_content["intersections"][0]
        else:
            intersection = None

        alerts_df = await zonal_statistics(geojson, aoi, intersection)

    if metadata_content["start_date"] is not None:
        alerts_df = alerts_df[alerts_df.alert_date >= metadata_content["start_date"]]
    if metadata_content["end_date"] is not None:
        alerts_df = alerts_df[alerts_df.alert_date <= metadata_content["end_date"]]
    alerts_dict = alerts_df.to_dict(orient="list")

    data = file_path / "data.json"
    data.write_text(json.dumps(alerts_dict))
