import logging

from prefect import flow, task
from prefect.logging import get_run_logger

from pipelines.disturbance.check_for_new_alerts import get_latest_version
from pipelines.intdist.create_zarr import create_zarr

logging.getLogger("distributed.client").setLevel(logging.ERROR)


@task
def get_new_intdist_version() -> str:
    return get_latest_version("gfw_integrated_dist_alerts")


@task
def create_zarr_task(dist_version: str, overwrite=False) -> str:
    zarr_uri = create_zarr(dist_version, overwrite=overwrite)
    return zarr_uri


@flow(
    name="Create int-Dist zarr",
    log_prints=True,
    description="Create zarr from tiles of one version of the integrated disturbance alerts dataset",
)
def intdist_zarr_flow(version=None, overwrite=False) -> list[str]:
    logger = get_run_logger()

    if version is None:
        version = get_new_intdist_version()
        logger.info(f"Latest int-dist version: {version}")

    dist_zarr_uri = create_zarr_task(version, overwrite=overwrite)

    return [dist_zarr_uri]
