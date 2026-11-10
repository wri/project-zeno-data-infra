import pyarrow.compute as pc
from prefect import flow
from prefect.logging import get_run_logger
from pyiceberg.catalog import load_catalog

from pipelines.viirs_fire_alerts.alert_cells import add_s2_cells
from pipelines.viirs_fire_alerts.feed_continuity import check_continuity
from pipelines.viirs_fire_alerts.firms_feed import download_feed, parse_feed
from pipelines.viirs_fire_alerts.iceberg_alert_store import IcebergAlertStore


@flow(name="VIIRS fire alerts table", retries=2, retry_delay_seconds=300)
def viirs_fire_alerts_flow(
    feed_url: str,
    catalog_name: str,
    table_name: str,
    coarse_cell_level: int = 2,
    fine_cell_level: int = 13,
) -> int:
    """Append the alerts in a FIRMS VIIRS feed that aren't already in the
    Iceberg alerts table, in one commit. Safe to re-run or run concurrently:
    stored alerts are skipped, and a failed run commits nothing. Fails without
    writing if days are missing between the stored alerts and the feed.

    Args:
        feed_url: URL of a FIRMS VIIRS CSV file, such as the global 7-day file.
        catalog_name: Iceberg catalog, configured through
            PYICEBERG_CATALOG__{CATALOG}__* environment variables.
        table_name: The alerts table, as namespace.table.
        coarse_cell_level: S2 level of the table's spatial partitions.
        fine_cell_level: S2 level used to order rows within files.

    Returns:
        The number of alerts appended.
    """
    logger = get_run_logger()
    alerts = parse_feed(download_feed(feed_url))
    if alerts.num_rows == 0:
        logger.info("no new alerts")
        return 0
    store = IcebergAlertStore(load_catalog(catalog_name), table_name)
    check_continuity(store.latest_date(), pc.min(alerts.column("acq_date")).as_py())
    appended = store.append_unseen(
        add_s2_cells(alerts, coarse_cell_level, fine_cell_level)
    )
    if appended == 0:
        logger.info("no new alerts")
    else:
        logger.info(f"Appended {appended} of {alerts.num_rows} feed alerts")
    return appended
