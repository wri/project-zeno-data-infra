from typing import Optional

from prefect import flow
from shapely.geometry import Polygon

from pipelines.integrated_alerts.alert_points_table import DAYS_STORED
from pipelines.integrated_alerts.prefect_flows import integrated_alerts_common_tasks
from pipelines.utils import s3_uri_exists


@flow(name="Integrated alerts points table", retries=2, retry_delay_seconds=120)
def integrated_alerts_points_table(
    integrated_alerts_zarr_uri: str,
    version: str,
    admin_result_uri: str,
    days_stored: int = DAYS_STORED,
    overwrite=False,
    bbox: Optional[Polygon] = None,
):
    """Write the alert points table: every integrated alert pixel in the
    days_stored days ending at the release's newest alert date, with its
    confidence and SBTN natural lands class, for listing the newest alerts in an
    area of interest.

    Args:
        integrated_alerts_zarr_uri: URI of the integrated alerts date_conf zarr.
        version: Integrated alerts version, used in the output S3 path and the
            table's metadata.
        admin_result_uri: The release's GADM integrated alerts parquet, whose
            alert_date statistics give the newest alert date.
        days_stored: Number of days of alerts to keep.
        overwrite: If True, rebuild and overwrite an existing S3 table.
        bbox: Optional polygon to limit the table to, for a small local test
            run. The table is then written to a local parquet
            (``alert-points-by-date-{version}.parquet``) instead of the S3 path.

    Returns:
        The URI of the table.
    """
    if bbox is None:
        result_uri = (
            f"{integrated_alerts_common_tasks.INTEGRATED_ALERTS_PREFIX}"
            f"/{version}/alert-points/by-date.parquet"
        )
        if not overwrite and s3_uri_exists(result_uri):
            return result_uri
    else:
        # local bbox test run
        result_uri = f"alert-points-by-date-{version}.parquet"

    integrated_alerts_common_tasks.build_alert_points_table.with_options(
        name="build-alert-points-table"
    )(
        integrated_alerts_zarr_uri,
        admin_result_uri,
        version,
        result_uri,
        int(days_stored),
        bbox,
    )
    return result_uri
