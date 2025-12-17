import logging

import boto3
from prefect import flow, task
from prefect.logging import get_run_logger

from pipelines.disturbance import prefect_flows, validate_zonal_statistics
from pipelines.disturbance.check_for_new_alerts import get_latest_version
from pipelines.disturbance.create_zarr import create_zarr as create_zarr_func
from pipelines.globals import ANALYTICS_BUCKET

logging.getLogger("distributed.client").setLevel(logging.ERROR)


@task
def get_new_dist_version() -> str:
    return get_latest_version("umd_glad_dist_alerts")


@task
def create_zarr(dist_version: str, overwrite=False) -> str:
    zarr_uri = create_zarr_func(dist_version, overwrite=overwrite)
    return zarr_uri


@task
def run_validation_suite(gadm_dist_result, version, contextual_layer=None) -> bool:
    return validate_zonal_statistics.validate(
        gadm_dist_result, version=version, contextual_layer=contextual_layer
    )


@task
def write_dist_latest_version(dist_version) -> None:
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=ANALYTICS_BUCKET,
        Key="zonal-statistics/dist-alerts/latest",
        Body=dist_version.encode("utf-8"),
        ContentType="text/plain",
    )


@flow(
    name="DIST alerts areas and validation",
    log_prints=True,
    description="Compute DIST alerts areas by GADM admin levels 0, 1, and 2, as well as by various contextual layers, and run validation suite on the results.",
)
def dist_alerts_flow(dist_version=None, overwrite=False, is_latest=False) -> list[str]:
    logger = get_run_logger()
    result_uris = []

    if dist_version is None:
        is_latest = True
        dist_version = get_new_dist_version()
        logger.info(f"Latest dist version: {dist_version}")

    dist_zarr_uri = create_zarr(dist_version, overwrite=overwrite)

    # Base GADM dist alerts
    gadm_dist_result = prefect_flows.dist_alerts_area(
        dist_zarr_uri, dist_version, overwrite=overwrite
    )
    validate_result = run_validation_suite(gadm_dist_result, version=dist_version)
    if not validate_result["validation_passed"]:
        raise ValueError(
            f"Validation for DIST area failed: {validate_result.get('details', {})}"
        )
    result_uris.append(gadm_dist_result)

    # GADM dist alerts by natural lands
    gadm_dist_by_natural_lands_result = prefect_flows.dist_alerts_by_natural_lands_area(
        dist_zarr_uri, dist_version, overwrite=overwrite
    )
    validate_natural_lands_result = run_validation_suite(
        gadm_dist_by_natural_lands_result,
        version=dist_version,
        contextual_layer=validate_zonal_statistics.NATURAL_LANDS,
    )
    if not validate_natural_lands_result["validation_passed"]:
        raise ValueError(
            f"DIST area by natural lands validation failed: {validate_natural_lands_result.get('details', {})}"
        )
    result_uris.append(gadm_dist_by_natural_lands_result)

    # GADM dist alerts by drivers
    gadm_dist_by_drivers_result = prefect_flows.dist_alerts_by_drivers_area(
        dist_zarr_uri, dist_version, overwrite=overwrite
    )
    validate_drivers_result = run_validation_suite(
        gadm_dist_by_drivers_result,
        version=dist_version,
        contextual_layer=validate_zonal_statistics.DIST_DRIVERS,
    )
    if not validate_drivers_result["validation_passed"]:
        raise ValueError(
            f"DIST area by drivers validation failed: {validate_drivers_result.get('details', {})}"
        )
    result_uris.append(gadm_dist_by_drivers_result)

    # GADM dist alerts by grasslands
    gadm_dist_by_grasslands_result = prefect_flows.dist_alerts_by_grasslands_area(
        dist_zarr_uri, dist_version, overwrite=overwrite
    )
    validate_grasslands_result = run_validation_suite(
        gadm_dist_by_grasslands_result,
        version=dist_version,
        contextual_layer=validate_zonal_statistics.GRASSLANDS,
    )
    if not validate_grasslands_result["validation_passed"]:
        raise ValueError(
            f"DIST area by grasslands validation failed: {validate_grasslands_result.get('details', {})}"
        )
    result_uris.append(gadm_dist_by_grasslands_result)

    # GADM dist alerts by land cover
    gadm_dist_by_land_cover_result = prefect_flows.dist_alerts_by_land_cover_area(
        dist_zarr_uri, dist_version, overwrite=overwrite
    )
    validate_land_cover_result = run_validation_suite(
        gadm_dist_by_land_cover_result,
        version=dist_version,
        contextual_layer=validate_zonal_statistics.LAND_COVER,
    )
    if not validate_land_cover_result["validation_passed"]:
        raise ValueError(
            f"DIST area by land cover validation failed: {validate_land_cover_result.get('details', {})}"
        )
    result_uris.append(gadm_dist_by_land_cover_result)

    if is_latest:
        write_dist_latest_version(dist_version)

    return result_uris


def main(overwrite=False):
    dist_alerts_flow(overwrite=overwrite)


if __name__ == "__main__":
    main(overwrite=False)
