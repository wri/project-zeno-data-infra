from datetime import date
from typing import Optional

import geopandas as gpd
import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger
from pyiceberg.catalog import load_catalog
from shapely.geometry import Polygon

from pipelines.globals import ANALYTICS_BUCKET, GADM_AOIS_URI
from pipelines.utils import s3_uri_exists, write_version
from pipelines.viirs_fire_alerts import admin_counts
from pipelines.viirs_fire_alerts.iceberg_alert_store import IcebergAlertStore

# Daily versions (vYYYYMMDD) of VIIRS fire alert counts by admin area
VIIRS_FIRE_ALERTS_PREFIX = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/viirs-fire-alerts"
RESULT_NAME = "admin-viirs-fire-alerts.parquet"


def result_uri(version: str) -> str:
    return f"{VIIRS_FIRE_ALERTS_PREFIX}/{version}/{RESULT_NAME}"


def latest_uri() -> str:
    return f"{VIIRS_FIRE_ALERTS_PREFIX}/latest"


@task(persist_result=False)
def read_alerts(table, newest_date: date, bbox: Optional[Polygon]) -> pd.DataFrame:
    alerts = admin_counts.read_alerts(table, newest_date, bbox)
    get_run_logger().info(
        f"Read {len(alerts):,} alerts from "
        f"{admin_counts.window_start(newest_date)} to {newest_date}"
    )
    return alerts


@task(persist_result=False)
def read_admin_units(uri: str, bbox: Optional[Polygon]) -> gpd.GeoDataFrame:
    units = admin_counts.read_finest_admin_units(uri, bbox)
    get_run_logger().info(f"Read {len(units):,} finest admin units")
    return units


@task(persist_result=False)
def count_alerts(alerts: pd.DataFrame, units: gpd.GeoDataFrame) -> pd.DataFrame:
    counts = admin_counts.admin_alert_counts(alerts, units)
    get_run_logger().info(
        f"{len(counts):,} rows for {counts.aoi_id.nunique():,} admin areas; "
        f"{matched_alerts(counts):,} of {len(alerts):,} alerts inside an admin area"
    )
    return counts


def matched_alerts(counts: pd.DataFrame) -> int:
    """Alerts counted in some admin area: the total over the countries."""
    return int(counts.alert_count[~counts.aoi_id.str.contains(".", regex=False)].sum())


@task(persist_result=False)
def write_counts(counts: pd.DataFrame, uri: str) -> None:
    admin_counts.write_counts(counts, uri)


# No retries of its own: it runs as a subflow of viirs_fire_alerts_flow, whose
# retries re-run it.
@flow(name="VIIRS fire alerts admin counts")
def viirs_admin_counts_flow(
    catalog_name: str,
    table_name: str,
    cell_level: int,
    overwrite: bool = False,
    bbox: Optional[Polygon] = None,
    gadm_aois_uri: str = GADM_AOIS_URI,
) -> Optional[str]:
    """Count VIIRS fire alerts by GADM admin area (levels 0-2), date and
    confidence over the last year, write the counts as version
    v<newest alert date> and mark that version latest. The version is skipped
    if it already exists, so a re-run with no new alerts does nothing.

    Args:
        catalog_name: Iceberg catalog holding the alerts table (see
            viirs_fire_alerts_flow).
        table_name: The alerts table, as namespace.table.
        cell_level: The table's S2 cell level (see viirs_fire_alerts_flow).
            Only appends use it, so it doesn't affect the counts.
        overwrite: Recompute the version even if its counts exist.
        bbox: Optional polygon to limit the alerts and admin units to, for a
            small local test run. The counts are then written to a local file
            (admin-viirs-fire-alerts-{version}.parquet) and latest is unchanged.
        gadm_aois_uri: GeoParquet of the GADM units.

    Returns:
        The URI of the counts, or None if the alerts table is empty.
    """
    logger = get_run_logger()
    store = IcebergAlertStore(load_catalog(catalog_name), table_name, cell_level)
    newest_date = store.latest_date()
    if newest_date is None:
        logger.info("No alerts stored, so no admin counts")
        return None
    version = f"v{newest_date:%Y%m%d}"

    if bbox is None:
        uri = result_uri(version)
        if not overwrite and s3_uri_exists(uri):
            logger.info(f"{uri} already exists")
            write_version(latest_uri(), version)
            return uri
    else:
        uri = f"admin-viirs-fire-alerts-{version}.parquet"

    alerts = read_alerts(store.table(), newest_date, bbox)
    units = read_admin_units(gadm_aois_uri, bbox)
    counts = count_alerts(alerts, units)
    del alerts, units
    write_counts(counts, uri)
    logger.info(f"Wrote {uri}")

    if bbox is None:
        write_version(latest_uri(), version)
    return uri
