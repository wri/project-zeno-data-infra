import logging

import boto3
from prefect import flow, task
from prefect.logging import get_run_logger

from pipelines.integrated_alerts.prefect_flows.gadm_integrated_alerts import integrated_alerts_area
from pipelines.disturbance.check_for_new_alerts import get_latest_version
from pipelines.integrated_alerts.create_zarr import create_zarr
from pipelines.globals import ANALYTICS_BUCKET

logging.getLogger("distributed.client").setLevel(logging.ERROR)


@task
def get_new_integrated_alerts_version() -> str:
    return get_latest_version("gfw_integrated_dist_alerts")


@task
def create_zarr_task(dist_version: str, overwrite=False) -> str:
    zarr_uri = create_zarr(dist_version, overwrite=overwrite)
    return zarr_uri

@task
def write_int_latest_version(version) -> None:
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=ANALYTICS_BUCKET,
        Key="zonal-statistics/integrated-alerts/latest",
        Body=version.encode("utf-8"),
        ContentType="text/plain",
    )


@flow(
    name="Create int-Dist zarr",
    log_prints=True,
    description="Create zarr from tiles of one version of the integrated disturbance alerts dataset",
)
def integrated_alerts_zarr_flow(version=None, overwrite=False, is_latest=False) -> list[str]:
    logger = get_run_logger()
    result_uris = []

    if version is None:
        is_latest = True
        version = get_new_integrated_alerts_version()
        logger.info(f"Latest int-dist version: {version}")

    integrated_alerts_zarr_uri = create_zarr_task(version, overwrite=overwrite)

    # Base GADM dist alerts
    gadm_dist_result = integrated_alerts_area(
        integrated_alerts_zarr_uri, version, overwrite=overwrite
    )
    result_uris.append(gadm_dist_result)

    if is_latest:
        write_int_latest_version(version)

    return result_uris
