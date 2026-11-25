"""Counts of VIIRS fire alerts by GADM admin area (levels 0-2), date and
confidence, in the layout of admin-integrated-alerts.parquet.

Alerts are joined to the finest admin units at levels 0-2 (a level-2 unit where
the country has them, otherwise level 1 or 0) with one geopandas spatial join,
then summed up to every coarser level by aoi_id prefix (BRA.14.3 -> BRA.14 ->
BRA), so each alert is tested against polygons once. This was the fastest of
the approaches compared in the VIIRS admin counts spike (about 25 s for a year
of alerts against all units, on one core).
"""

from datetime import date, timedelta
from typing import Optional

import geopandas as gpd
import pandas as pd
import pyarrow.compute as pc
from pyiceberg.expressions import (
    And,
    BooleanExpression,
    GreaterThanOrEqual,
    LessThanOrEqual,
)
from pyiceberg.table import Table
from shapely.geometry import Polygon

WINDOW_DAYS = 365
ALERT_COLUMNS = ("longitude", "latitude", "acq_date", "confidence")
GROUP_COLUMNS = ["aoi_id", "alert_date", "alert_confidence"]
OUTPUT_COLUMNS = ["aoi_id", "aoi_type", "alert_date", "alert_confidence", "alert_count"]
CONFIDENCE_NAMES = {"h": "high", "n": "nominal", "l": "low"}


def window_start(newest_date: date) -> date:
    """The first alert date counted: WINDOW_DAYS days ending on newest_date."""
    return newest_date - timedelta(days=WINDOW_DAYS - 1)


def read_alerts(
    table: Table, newest_date: date, bbox: Optional[Polygon] = None
) -> pd.DataFrame:
    """Read the WINDOW_DAYS days of alerts ending on newest_date, optionally only
    those in a bbox (for a small local test run)."""
    row_filter: BooleanExpression = And(
        GreaterThanOrEqual("acq_date", window_start(newest_date).isoformat()),
        LessThanOrEqual("acq_date", newest_date.isoformat()),
    )
    if bbox is not None:
        west, south, east, north = bbox.bounds
        row_filter = And(
            row_filter,
            GreaterThanOrEqual("longitude", west),
            LessThanOrEqual("longitude", east),
            GreaterThanOrEqual("latitude", south),
            LessThanOrEqual("latitude", north),
        )
    rows = table.scan(row_filter=row_filter, selected_fields=ALERT_COLUMNS).to_arrow()
    return (
        rows.select(list(ALERT_COLUMNS)).to_pandas().astype({"confidence": "category"})
    )


def read_finest_admin_units(
    uri: str, bbox: Optional[Polygon] = None
) -> gpd.GeoDataFrame:
    """Read the GADM units at levels 0-2 from the AOI GeoParquet and keep the
    finest ones, optionally only those intersecting a bbox. A bbox read uses the
    file's covering bbox column, so it reads only nearby units."""
    units = gpd.read_parquet(
        uri,
        columns=["aoi_id", "adm_level", "geometry"],
        filters=pc.field("adm_level") <= 2,
        bbox=None if bbox is None else bbox.bounds,
    )
    finest = select_finest_units(units)
    if bbox is not None:
        finest = finest[finest.intersects(bbox)].reset_index(drop=True)
    return finest


def select_finest_units(units: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Select the units that have no children among these units, so every point
    is counted once and coarser levels are sums of these."""
    parent_ids = units.aoi_id.str.rsplit(".", n=1).str[0].where(units.adm_level > 0)
    return units[~units.aoi_id.isin(parent_ids)][["aoi_id", "geometry"]].reset_index(
        drop=True
    )


def count_by_finest_unit(alerts: pd.DataFrame, units: gpd.GeoDataFrame) -> pd.DataFrame:
    """Count alerts per finest unit, date and confidence. Alerts outside every
    unit (mostly offshore) are dropped, and so is a point exactly on a shared
    edge, since it is within neither unit."""
    points = gpd.GeoDataFrame(
        {
            "alert_date": alerts.acq_date.to_numpy(),
            "alert_confidence": alerts.confidence.to_numpy(),
        },
        geometry=gpd.points_from_xy(alerts.longitude, alerts.latitude),
        crs=units.crs or 4326,
    )
    joined = points.sjoin(units[["aoi_id", "geometry"]], predicate="within")
    return (
        joined.groupby(GROUP_COLUMNS, observed=True)
        .size()
        .rename("alert_count")
        .reset_index()
    )


def roll_up_counts(counts: pd.DataFrame) -> pd.DataFrame:
    """Sum finest-unit counts to every admin level, with the output columns
    and sorted by aoi_id, which API admin queries filter on."""
    unit_of_row, unit_ids = pd.factorize(counts.aoi_id)
    id_parts = pd.Series(unit_ids).str.split(".")
    levels = []
    for depth in (1, 2, 3):
        ancestor_ids = (
            id_parts.str[:depth].str.join(".").where(id_parts.str.len() >= depth)
        )
        ancestor_of_row = ancestor_ids.to_numpy()[unit_of_row]
        has_ancestor = pd.notna(ancestor_of_row)
        levels.append(
            counts[has_ancestor]
            .assign(aoi_id=ancestor_of_row[has_ancestor])
            .groupby(GROUP_COLUMNS, observed=True)
            .alert_count.sum()
            .reset_index()
        )
    rolled = pd.concat(levels, ignore_index=True)
    rolled["aoi_type"] = "admin"
    rolled["alert_confidence"] = (
        rolled.alert_confidence.astype(str).map(CONFIDENCE_NAMES).astype("category")
    )
    rolled["alert_count"] = rolled.alert_count.astype("int64")
    return rolled[OUTPUT_COLUMNS].sort_values(
        ["aoi_id", "alert_confidence", "alert_date"], ignore_index=True
    )


def admin_alert_counts(alerts: pd.DataFrame, units: gpd.GeoDataFrame) -> pd.DataFrame:
    """Alert counts for every admin area at levels 0-2."""
    return roll_up_counts(count_by_finest_unit(alerts, units))


def write_counts(counts: pd.DataFrame, uri: str) -> None:
    """Write counts as Parquet, in row groups small enough that a query for one
    aoi_id reads little more than its own rows."""
    counts.to_parquet(uri, index=False, row_group_size=100_000)
