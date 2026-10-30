"""Integrated alerts zonal statistics for vector areas of interest: alert area
by SBTN natural lands class, alert date and confidence, per AOI, in the same
schema and order as the GADM results (admin-integrated-alerts.parquet).

Computed by testing alert pixel centers against the AOI polygons
(pipelines/zonal_stats/pixels_in_polygons.py): a tile's candidate pixels are its
integrated alert pixels, each with its pixel area."""

from datetime import date
from functools import partial
from typing import Dict, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import xarray as xr
from shapely.geometry import Polygon

from pipelines.globals import pixel_area_10m_zarr_uri, sbtn_natural_lands_zarr_uri
from pipelines.integrated_alerts.alert_pixels import (
    ALERT_EPOCH,
    alert_tiles,
    open_zarrs,
    read_tile_alert_pixels,
)
from pipelines.integrated_alerts.stages import alerts_confidence
from pipelines.prefect_flows.tile_stages import (
    window_bounds,
    worker_cached,
)
from pipelines.zonal_stats.aoi_sources import AOISource
from pipelines.zonal_stats.pixels_in_polygons import zonal_stats

# Same dates as the GADM results: from 2023-01-01 to the last alert_date code.
FIRST_DAY = (date(2023, 1, 1) - ALERT_EPOCH).days
LAST_DAY = 4999
BY = ["natural_lands_class", "alert_date", "confidence"]
# layers sampled at each alert pixel, by column name
LAYER_URIS = {
    "natural_lands_class": sbtn_natural_lands_zarr_uri,
    "area_m2": pixel_area_10m_zarr_uri,
}
RESULT_SCHEMA = pa.schema(
    [
        ("aoi_id", pa.string()),
        ("aoi_type", pa.string()),
        ("natural_lands_class", pa.uint8()),
        ("alert_date", pa.date32()),
        ("alert_confidence", pa.string()),
        ("area_ha", pa.float64()),
    ]
)


def read_alert_pixels(ys: slice, xs: slice, alerts_zarr_uri: str) -> pd.DataFrame:
    """Read a tile's alert pixels from FIRST_DAY on, with their location,
    natural lands class and area."""
    zarrs = worker_cached(
        ("vector_zonal_stats_zarrs", alerts_zarr_uri),
        partial(open_zarrs, alerts_zarr_uri, LAYER_URIS),
    )
    pixels = read_tile_alert_pixels(zarrs, ys, xs, FIRST_DAY, LAST_DAY)
    return pixels.assign(area_ha=pixels.pop("area_m2").astype(np.float64) / 10_000)


def label_results(sums: pd.DataFrame, aoi_type: str) -> pd.DataFrame:
    """Format summed alert areas like the GADM results."""
    return pd.DataFrame(
        {
            "aoi_id": sums["aoi_id"],
            "aoi_type": aoi_type,
            "natural_lands_class": sums["natural_lands_class"].astype(np.uint8),
            "alert_date": (
                pd.Timestamp(ALERT_EPOCH)
                + pd.to_timedelta(sums["alert_date"].astype(np.int64), unit="D")
            ).dt.date,
            "alert_confidence": sums["confidence"].map(alerts_confidence),
            "area_ha": sums["area_ha"],
        }
    ).sort_values(
        ["aoi_id", "natural_lands_class", "alert_confidence", "alert_date"],
        ignore_index=True,
    )


def build_vector_zonal_stats(
    aoi_source: AOISource,
    alerts_zarr_uri: str,
    version: str,
    result_uri: str,
    bbox: Optional[Polygon] = None,
) -> Dict:
    """Compute integrated alerts zonal statistics for every AOI in aoi_source
    and write them to result_uri. With a bbox, only alert pixels inside it
    count."""
    grid = xr.open_zarr(alerts_zarr_uri)["alert_date"].squeeze("band", drop=True)
    tiles = [
        (ys, xs, window_bounds(grid, ys, xs))
        for ys, xs in alert_tiles(alerts_zarr_uri, bbox)
    ]
    return zonal_stats(
        aoi_source,
        tiles,
        read_pixels=partial(read_alert_pixels, alerts_zarr_uri=alerts_zarr_uri),
        by=BY,
        sum_column="area_ha",
        finish=partial(label_results, aoi_type=aoi_source.aoi_type),
        schema=RESULT_SCHEMA,
        result_uri=result_uri,
        metadata={"alerts_version": version},
    )
