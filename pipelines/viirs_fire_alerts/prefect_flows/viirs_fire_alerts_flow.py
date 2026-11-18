import os
from datetime import datetime, timedelta, timezone

import httpx
import pyarrow as pa
from prefect import flow
from prefect.logging import get_run_logger
from pyiceberg.catalog import load_catalog

from pipelines.viirs_fire_alerts.firms_area_api import (
    FirmsAreaApi,
    check_continuity,
    parse_feed,
    request_windows,
)
from pipelines.viirs_fire_alerts.iceberg_alert_store import IcebergAlertStore


def connect_firms(source: str) -> FirmsAreaApi:
    """The FIRMS area API, authenticated with the FIRMS_MAP_KEY environment
    variable."""
    return FirmsAreaApi(
        httpx.Client(timeout=300, follow_redirects=True),
        os.environ["FIRMS_MAP_KEY"],
        source,
    )


@flow(name="VIIRS fire alerts table", retries=2, retry_delay_seconds=300)
def viirs_fire_alerts_flow(
    source: str,
    catalog_name: str,
    table_name: str,
    cell_level: int = 13,
) -> int:
    """Append, in one commit, FIRMS alerts from the newest stored day through
    today that aren't already in the Iceberg alerts table, so missed days are
    filled automatically. An empty table starts from the last five days. Safe
    to re-run or run concurrently: stored alerts are skipped, and a failed run
    commits nothing. Fails without writing if the missing days are older than
    the source keeps.

    Args:
        source: FIRMS source, such as VIIRS_NOAA20_NRT.
        catalog_name: Iceberg catalog, configured through
            PYICEBERG_CATALOG__<NAME>__* environment variables. Deployed flows
            use the S3 Tables catalog terraform sets as PYICEBERG_CATALOG__AOIS__*.
        table_name: The alerts table, as namespace.table.
        cell_level: S2 level used to order rows within files.

    Returns:
        The number of alerts appended.
    """
    logger = get_run_logger()
    firms = connect_firms(source)
    store = IcebergAlertStore(load_catalog(catalog_name), table_name, cell_level)
    latest = store.latest_date()
    check_continuity(latest, firms.first_available_date())
    today = datetime.now(timezone.utc).date()
    start = latest or today - timedelta(days=4)
    feeds = [
        parse_feed(firms.download(first_day, days))
        for first_day, days in request_windows(start, today)
    ]
    alerts = pa.concat_tables(feeds)
    appended = store.append_new(alerts)
    if appended == 0:
        logger.info("no new alerts")
    else:
        logger.info(f"Appended {appended} of {alerts.num_rows} feed alerts")
    return appended
