import logging

import coiled
from prefect import flow, task
from prefect.logging import get_run_logger

from pipelines.disturbance.create_zarr import (
    create_zarr as create_zarr_func,
)
from pipelines.disturbance.check_for_new_alerts import get_latest_version
from pipelines.disturbance import validate_zonal_statistics

from pipelines.disturbance import prefect_flows
from pipelines.natural_lands.prefect_flows import nl_flow as nl_prefect_flow
from pipelines.grasslands.prefect_flows import grasslands_flow

logging.getLogger("distributed.client").setLevel(logging.ERROR)


@task
def get_new_dist_version() -> str:
    return get_latest_version("umd_glad_dist_alerts")


@task
def create_cluster():
    cluster = coiled.Cluster(
        name="dist_alerts_zonal_stat_count",
        region="us-east-1",
        n_workers=10,
        tags={"project": "dist_alerts_zonal_stat"},
        scheduler_vm_types=["r7g.xlarge"],
        worker_vm_types=["r7g.2xlarge"],
        compute_purchase_option="spot_with_fallback",
        no_client_timeout="5 seconds",
        container="globalforestwatch/zeno:2",
    )
    cluster.adapt(minimum=10, maximum=50)

    client = cluster.get_client()
    return client, cluster


@task
def create_zarr(dist_version: str, overwrite=False) -> str:
    zarr_uri = create_zarr_func(dist_version, overwrite=overwrite)
    return zarr_uri


@task
def run_validation_suite(gadm_dist_result):
    validate_zonal_statistics.validate(gadm_dist_result)


@flow(name="dist_alerts_flow", log_prints=True)
def dist_alerts_flow(overwrite=False) -> list[str]:
    logger = get_run_logger()
    dask_client = None
    result_uris = []
    try:
        dist_version = get_new_dist_version()
        logger.info(f"Latest dist version: {dist_version}")
        dask_client, _ = create_cluster()

        gl_result = grasslands_flow.gadm_grasslands_area(overwrite=overwrite)
        result_uris.append(gl_result)

        nl_result = nl_prefect_flow.gadm_natural_lands_area(overwrite=overwrite)
        result_uris.append(nl_result)

        dist_zarr_uri = create_zarr(dist_version, overwrite=overwrite)
        gadm_dist_result = prefect_flows.dist_alerts_count(
            dist_zarr_uri, dist_version, overwrite=overwrite
        )
        result_uris.append(gadm_dist_result)

        gadm_dist_by_natural_lands_result = (
            prefect_flows.dist_alerts_by_natural_lands_count(
                dist_zarr_uri, dist_version, overwrite=overwrite
            )
        )
        result_uris.append(gadm_dist_by_natural_lands_result)

        gadm_dist_by_drivers_result = prefect_flows.dist_alerts_by_drivers_count(
            dist_zarr_uri, dist_version, overwrite=overwrite
        )
        result_uris.append(gadm_dist_by_drivers_result)

        gadm_dist_by_grasslands_result = (
            prefect_flows.dist_alerts_by_grasslands_count(
                dist_zarr_uri, dist_version, overwrite=overwrite
            )
        )
        result_uris.append(gadm_dist_by_grasslands_result)

        validate_result = run_validation_suite(gadm_dist_result)

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
