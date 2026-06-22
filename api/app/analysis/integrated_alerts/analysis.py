from functools import partial
from typing import Dict

import dask.dataframe as dd
import duckdb
import numpy as np
import pandas as pd
import xarray as xr
from dask.dataframe import DataFrame as DaskDataFrame
from flox.xarray import xarray_reduce

from ..common.analysis import (
    JULIAN_DATE_2021,
    get_geojson,
    initialize_duckdb,
    read_zarr_clipped_to_geojson,
)
from .query import create_gadm_integrated_alerts_query

ALERTS_CONFIDENCE = {2: "low", 3: "high"}


def integrated_alerts_zarr_uri(version: str) -> str:
    return (
        f"s3://lcl-analytics/zarr/gfw_integrated_dist_alerts/{version}/date_conf.zarr"
    )


async def zonal_statistics_on_aois(
    input_uris: Dict[str, str], aois, dask_client, version
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

    precompute_partial = partial(zonal_statistics, input_uris, version=version)
    futures = dask_client.map(precompute_partial, aois, geojsons)
    dd_df_futures = await dask_client.gather(futures)
    dfs = await dask_client.gather(dd_df_futures)
    alerts_df = await dask_client.compute(dd.concat(dfs))

    return alerts_df


async def zonal_statistics(
    input_uris: Dict[str, str],
    aoi,
    geojson,
    version: str,
) -> DaskDataFrame:
    alerts = read_zarr_clipped_to_geojson(integrated_alerts_zarr_uri(version), geojson)

    pixel_area = read_zarr_clipped_to_geojson(
        input_uris["pixel_area_zarr_uri"], geojson
    ).band_data.reindex_like(alerts, method="nearest", tolerance=1e-5)

    groupby_layers = [alerts.alert_date, alerts.confidence]
    expected_groups = [
        np.arange(5000),  # days since 2020-12-31 (epoch matches the GADM parquet)
        [2, 3],  # confidence: 2=low, 3=high
    ]

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
    alerts_df = alerts_df.rename(columns={"confidence": "alert_confidence"})
    alerts_df.alert_confidence = alerts_df.alert_confidence.map(
        ALERTS_CONFIDENCE, meta=("alert_confidence", "object")
    )
    alerts_df.alert_date = dd.to_datetime(
        alerts_df.alert_date + JULIAN_DATE_2021, origin="julian", unit="D"
    ).dt.strftime("%Y-%m-%d")

    alerts_df["aoi_type"] = aoi["type"].lower()
    alerts_df["aoi_id"] = aoi["id"] if "id" in aoi else aoi["properties"]["id"]

    alerts_df = alerts_df[alerts_df.area_ha > 0]
    return alerts_df


async def get_precomputed_statistics(aoi, dask_client, version: str):
    if aoi["type"] != "admin":
        raise ValueError(
            f"No precomputed statistics available for AOI type {aoi['type']}"
        )

    table = get_precomputed_table(version)
    precompute_partial = partial(get_precomputed_statistic_on_gadm_aoi, table=table)
    futures = dask_client.map(precompute_partial, aoi["ids"])
    results = await dask_client.gather(futures)
    alerts_df = pd.concat(results)

    return alerts_df


def get_precomputed_table(version: str) -> str:
    return (
        "s3://lcl-analytics/zonal-statistics/intdist-alerts/"
        f"{version}/admin-intdist-alerts.parquet"
    )


async def get_precomputed_statistic_on_gadm_aoi(id, table):
    # GADM IDs are coming joined by '.', e.g. IDN.24.9
    gadm_id = id.split(".")

    initialize_duckdb()
    query = create_gadm_integrated_alerts_query(gadm_id, table)
    alerts_df = duckdb.query(query).df()

    alerts_df["aoi_id"] = id
    alerts_df["aoi_type"] = "admin"

    return alerts_df
