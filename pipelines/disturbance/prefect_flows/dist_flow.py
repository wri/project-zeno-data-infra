import logging

from prefect import flow, task
from prefect.logging import get_run_logger

from pipelines.disturbance import prefect_flows, validate_zonal_statistics
from pipelines.disturbance.check_for_new_alerts import get_latest_version
from pipelines.disturbance.create_zarr import create_zarr as create_zarr_func

logging.getLogger("distributed.client").setLevel(logging.ERROR)


@task
def get_new_dist_version() -> str:
    return get_latest_version("umd_glad_dist_alerts")


@task
def create_zarr(dist_version: str, overwrite=False) -> str:
    zarr_uri = create_zarr_func(dist_version, overwrite=overwrite)
    return zarr_uri


@task
def run_validation_suite(gadm_dist_result):
    validate_zonal_statistics.validate(gadm_dist_result)


@flow(name="DIST alerts", log_prints=True)
def dist_alerts_flow(overwrite=False) -> list[str]:
    logger = get_run_logger()
    dask_client = None
    result_uris = []
    try:
        dist_version = get_new_dist_version()
        logger.info(f"Latest dist version: {dist_version}")

        dist_zarr_uri = create_zarr(dist_version, overwrite=overwrite)
        gadm_dist_result = prefect_flows.dist_alerts_area(
            dist_zarr_uri, dist_version, overwrite=overwrite
        )
        result_uris.append(gadm_dist_result)

        gadm_dist_by_natural_lands_result = (
            prefect_flows.dist_alerts_by_natural_lands_area(
                dist_zarr_uri, dist_version, overwrite=overwrite
            )
        )
        result_uris.append(gadm_dist_by_natural_lands_result)

        gadm_dist_by_drivers_result = prefect_flows.dist_alerts_by_drivers_area(
            dist_zarr_uri, dist_version, overwrite=overwrite
        )
        result_uris.append(gadm_dist_by_drivers_result)

        gadm_dist_by_grasslands_result = prefect_flows.dist_alerts_by_grasslands_area(
            dist_zarr_uri, dist_version, overwrite=overwrite
        )
        result_uris.append(gadm_dist_by_grasslands_result)

        gadm_dist_by_land_cover_result = prefect_flows.dist_alerts_by_land_cover_area(
            dist_zarr_uri, dist_version, overwrite=overwrite
        )
        result_uris.append(gadm_dist_by_land_cover_result)

        run_validation_suite(gadm_dist_result)

    except Exception:
        logger.error("DIST alerts analysis failed.")
        raise
    finally:
        if dask_client:
            dask_client.shutdown()

    return result_uris


def main(overwrite=False):
    dist_alerts_flow(overwrite=overwrite)


if __name__ == "__main__":
    main(overwrite=False)
