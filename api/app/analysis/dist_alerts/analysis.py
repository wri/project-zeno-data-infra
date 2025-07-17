import json
import os

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
        natural_lands.name = "natural_lands_class"

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

    if "id" in aoi:
        alerts_df[aoi["type"]] = aoi["id"]

    if intersection == "natural_lands":
        alerts_df["natural_lands_category"] = alerts_df.natural_lands_class.apply(
            lambda x: "natural" if 1 < x < 12 else "non-natural"
        )
        alerts_df["natural_lands_class"] = alerts_df.natural_lands_class.apply(
            lambda x: NATURAL_LANDS_CLASSES.get(x, "Unclassified")
        )
    elif intersection == "driver":
        alerts_df.ldacs_driver = alerts_df.ldacs_driver.apply(
            lambda x: DIST_DRIVERS.get(x, "Unclassified")
        )

    alerts_df = alerts_df[alerts_df.value > 0]
    return alerts_df


async def do_analytics(file_path):
    # Read and parse JSON file
    metadata = file_path / "metadata.json"
    json_content = metadata.read_text()
    metadata_content = json.loads(json_content)  # Convert JSON to Python object
    aoi = metadata_content["aois"][0]
    if aoi["type"] == "admin":
        # GADM IDs are coming joined by '.', e.g. IDN.24.9
        gadm_id = aoi["id"].split(".")
        intersections = metadata_content["intersections"]

        query, table = create_gadm_dist_query(gadm_id, intersections)

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
        alerts_df = duckdb.query(query).df()
        os.remove(f"/tmp/{table}.parquet")
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
