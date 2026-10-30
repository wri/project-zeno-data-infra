"""Integrated alerts zonal statistics for vector areas of interest: alert area
by SBTN natural lands class, alert date and confidence, per AOI, in the same
schema and order as the GADM results (admin-integrated-alerts.parquet).

Computed by testing alert pixel centers against the AOI polygons
(pipelines/zonal_stats/pixels_in_polygons.py): a tile's candidate pixels are its
integrated alert pixels, each with its pixel area."""

from datetime import date
from functools import partial
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import xarray as xr
from shapely.geometry import Polygon

from pipelines.globals import pixel_area_10m_zarr_uri, sbtn_natural_lands_zarr_uri
from pipelines.integrated_alerts.alert_pixels import (
    ALERT_EPOCH,
    list_alert_tiles,
    open_zarrs,
    read_tile_alert_pixels,
)
from pipelines.integrated_alerts.stages import alerts_confidence
from pipelines.prefect_flows.common_stages import rollup_by_gadm_and_convert_to_aoi
from pipelines.prefect_flows.tile_stages import (
    window_bounds,
    worker_cached,
)
from pipelines.zonal_stats.aoi_sources import AOISource, GeoParquetSource
from pipelines.zonal_stats.pixels_in_polygons import Tile, compute_zonal_stats

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


def compute_vector_zonal_stats(
    aoi_source: AOISource,
    alerts_zarr_uri: str,
    version: str,
    result_uri: str,
    bbox: Optional[Polygon] = None,
) -> Dict:
    """Compute integrated alerts zonal statistics for every AOI in aoi_source
    and write them to result_uri. With a bbox, only alert pixels inside it
    count."""
    return compute_zonal_stats(
        aoi_source,
        list_alert_tiles_with_bounds(alerts_zarr_uri, bbox),
        read_pixels=partial(read_alert_pixels, alerts_zarr_uri=alerts_zarr_uri),
        by=BY,
        sum_column="area_ha",
        finish=partial(label_results, aoi_type=aoi_source.aoi_type),
        schema=RESULT_SCHEMA,
        result_uri=result_uri,
        metadata={"alerts_version": version},
    )


def compute_admin_zonal_stats(
    aoi_source: GeoParquetSource,
    alerts_zarr_uri: str,
    version: str,
    result_uri: str,
    bbox: Optional[Polygon] = None,
) -> Dict:
    """Compute integrated alerts zonal statistics for admin units (GADM) and
    write them to result_uri. Only units without children are computed from
    pixels; every parent is summed from its children, one country at a time, as
    in the rasterized GADM results. A parent whose children don't cover it all
    gets only its children's totals."""
    parents = parent_ids(aoi_source.read_attributes(["aoi_id"])["aoi_id"])
    return compute_zonal_stats(
        aoi_source,
        list_alert_tiles_with_bounds(alerts_zarr_uri, bbox),
        read_pixels=partial(read_alert_pixels, alerts_zarr_uri=alerts_zarr_uri),
        by=BY,
        sum_column="area_ha",
        finish=partial(label_admin_results, aoi_type=aoi_source.aoi_type),
        schema=RESULT_SCHEMA,
        result_uri=result_uri,
        metadata={"alerts_version": version},
        exclude_ids=parents,
        bucket_key=country,
    )


def label_admin_results(sums: pd.DataFrame, aoi_type: str) -> pd.DataFrame:
    """Roll summed admin units up to every parent, then format them like the
    GADM results."""
    rolled_up = rollup_by_gadm_and_convert_to_aoi(split_admin_levels(sums), BY)
    return label_results(rolled_up, aoi_type)


def parent_ids(aoi_ids: pd.Series) -> pd.Index:
    """Return the admin units among aoi_ids that have children (ids encode the
    hierarchy: AFG.1.5 -> AFG.1 -> AFG)."""
    children = aoi_ids[aoi_ids.str.contains(".", regex=False)]
    return pd.Index(children.str.rsplit(".", n=1).str[0].unique())


def split_admin_levels(sums: pd.DataFrame) -> pd.DataFrame:
    """Split aoi_id into the country, region and subregion columns that
    rollup_by_gadm_and_convert_to_aoi takes (0 where a unit has no such
    level)."""
    levels = sums["aoi_id"].str.split(".", expand=True).reindex(columns=[0, 1, 2])
    return sums.drop(columns="aoi_id").assign(
        country=levels[0],
        region=levels[1].fillna(0),
        subregion=levels[2].fillna(0),
    )


def country(aoi_ids: pd.Series) -> pd.Series:
    """Return the country (level 0 id) of admin units."""
    return aoi_ids.str.split(".", n=1).str[0]


def list_alert_tiles_with_bounds(
    alerts_zarr_uri: str, bbox: Optional[Polygon] = None
) -> List[Tile]:
    """List the alerts zarr's tiles with data, as list_alert_tiles does, each
    with its bounds."""
    grid = xr.open_zarr(alerts_zarr_uri)["alert_date"].squeeze("band", drop=True)
    return [
        (ys, xs, window_bounds(grid, ys, xs))
        for ys, xs in list_alert_tiles(alerts_zarr_uri, bbox)
    ]
