import logging
import os

import boto3
from prefect import flow, task
from prefect.events import emit_event
from prefect.logging import get_run_logger

from pipelines.data_api import DataApiClient
from pipelines.globals import ANALYTICS_BUCKET
from pipelines.integrated_alerts.alert_points_table import DAYS_STORED
from pipelines.integrated_alerts.create_zarr import create_zarr
from pipelines.integrated_alerts.extra_processing import (
    extra_processing_tasks,
    is_first_sunday_week,
)
from pipelines.integrated_alerts.prefect_flows.alert_points_table import (
    integrated_alerts_points_table,
)
from pipelines.integrated_alerts.prefect_flows.gadm_integrated_alerts import (
    integrated_alerts_area,
)

logging.getLogger("distributed.client").setLevel(logging.ERROR)


@task
def get_new_integrated_alerts_version() -> str:
    return DataApiClient().get_latest_version("gfw_integrated_dist_alerts")


@task
def create_zarr_task(dist_version: str, overwrite=False) -> tuple[str, bool]:
    return create_zarr(dist_version, overwrite=overwrite)


@task
def extra_processing_task(version, zarr_uri) -> None:
    extra_processing_tasks(version, zarr_uri)


@task
def write_int_latest_version(version) -> None:
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=ANALYTICS_BUCKET,
        Key="zonal-statistics/integrated-alerts/latest",
        Body=version.encode("utf-8"),
        ContentType="text/plain",
    )


@task
def emit_int_latest_version_event(version) -> None:
    """Tell Prefect a new integrated alerts version is the latest, which starts
    the subscription emails (the run_subscription_emails_on_intdist_latest
    automation in terraform/prefect.tf)."""
    # TODO: enable once the subscription emails are ready to go out.
    if False:
        # Event names have the same Terraform workspace suffix as the automation.
        workspace = os.getenv("TF_WORKSPACE", "default")
        suffix = "" if workspace == "default" else f"-{workspace}"
        emit_event(
            event=f"intdist_latest{suffix}",
            resource={
                "prefect.resource.id": f"gfw_integrated_dist_alerts/{version}",
            },
            payload={"version": version},
        )


@flow(
    name="Create int-Dist zarr",
    log_prints=True,
    description=(
        "Create zarr from tiles of one version of the integrated disturbance "
        "alerts dataset"
    ),
)
def integrated_alerts_zarr_flow(
    version=None,
    overwrite=False,
    is_latest=False,
    bbox=None,
    days_stored=DAYS_STORED,
) -> list[str]:
    """bbox (a shapely Polygon) only clips the GADM parquet and the alert points
    table, which are then written to local parquet files. The zarr is still
    created for the whole world. days_stored is the number of days of alerts the
    alert points table keeps."""
    logger = get_run_logger()
    result_uris = []

    if version is None:
        is_latest = True
        version = get_new_integrated_alerts_version()
        logger.info(f"Latest int-dist version: {version}")

    integrated_alerts_zarr_uri, freshly_created = create_zarr_task(
        version, overwrite=overwrite
    )

    # The extra processing for the first Sunday of the month only depends on the
    # intdist_tropics.tif being created by the datapump and the int-dist zarr just
    # being created above. It may take a while because it does several copies of
    # large files (the zarr and the COG). So, we run it in parallel with the main
    # computation of the integrated alerts parquet.
    extra_processing_future = None
    if freshly_created and is_first_sunday_week(version):
        extra_processing_future = extra_processing_task.submit(
            version, integrated_alerts_zarr_uri
        )

    # Base GADM integrated alerts with breakdown by natural lands categories.
    gadm_dist_result = integrated_alerts_area(
        integrated_alerts_zarr_uri, version, overwrite=overwrite, bbox=bbox
    )
    result_uris.append(gadm_dist_result)

    # The API serves the latest version's alert points table, so it must exist
    # before the version is marked latest.
    points_table_result = integrated_alerts_points_table(
        integrated_alerts_zarr_uri,
        version,
        gadm_dist_result,
        days_stored=days_stored,
        overwrite=overwrite,
        bbox=bbox,
    )
    result_uris.append(points_table_result)

    # Don't mark a version as latest from a bbox run, since the API would then
    # look for a global parquet for this version that may not exist.
    if is_latest and bbox is None:
        write_int_latest_version(version)
        emit_int_latest_version_event(version)

    if extra_processing_future is not None:
        # Propagates failure, so fails the flow if the extra processing
        # fails, but only after the parquet processing.
        extra_processing_future.result()

    return result_uris
