import logging

import coiled
from prefect import flow, task
from prefect.logging import get_run_logger

from pipelines.dist.create_zarr import (
    create_zarr as create_zarr_func,
)
from pipelines.dist.check_for_new_alerts import get_latest_version

from pipelines.dist.gadm_dist_alerts import gadm_dist_alerts
from pipelines.dist.gadm_dist_alerts_by_natural_lands import (
    gadm_dist_alerts_by_natural_lands,
)
from pipelines.dist.gadm_dist_alerts_by_driver import gadm_dist_alerts_by_driver

DATA_LAKE_BUCKET = "gfw-data-lake"

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
        container="globalforestwatch/zeno:2",
        tags={"project": "dist_alerts_zonal_stat"},
        scheduler_vm_types=["r7g.xlarge"],
        worker_vm_types=["r7g.2xlarge"],
        compute_purchase_option="spot_with_fallback",
        no_client_timeout="5 seconds",
    )
    cluster.adapt(minimum=10, maximum=50)

    client = cluster.get_client()
    return client, cluster


@task
def create_zarr(dist_version: str, overwrite=False) -> str:
    zarr_uri = create_zarr_func(dist_version, overwrite=overwrite)
    return zarr_uri


@task
def analyze_gadm_dist(dist_zarr_uri, version, overwrite: bool):

    return gadm_dist_alerts(dist_zarr_uri, version, overwrite)


@task
def analyze_gadm_dist_by_natural_lands(dist_zarr_uri, version, overwrite: bool):

    return gadm_dist_alerts_by_natural_lands(dist_zarr_uri, version, overwrite)


@task
def analyze_gadm_dist_by_driver(dist_zarr_uri, version, overwrite: bool):

    return gadm_dist_alerts_by_driver(dist_zarr_uri, version, overwrite)


@task
def run_validation_suite():
    pass


@flow(name="DIST alerts count")
def main(overwrite=False) -> list[str]:
    logger = get_run_logger()
    dask_client = None
    result_uris = []
    try:
        dist_version = get_new_dist_version()
        dask_client, _ = create_cluster()
        dist_zarr_uri = create_zarr(dist_version, overwrite=overwrite)
        gadm_dist_result = analyze_gadm_dist(dist_zarr_uri, dist_version, overwrite=overwrite)
        result_uris.append(gadm_dist_result)

        gadm_dist_by_natural_lands_result = analyze_gadm_dist_by_natural_lands(
            dist_zarr_uri, dist_version, overwrite
        )
        result_uris.append(gadm_dist_by_natural_lands_result)

        gadm_dist_by_driver_result = analyze_gadm_dist_by_driver(
            dist_zarr_uri, dist_version, overwrite
        )
        result_uris.append(gadm_dist_by_driver_result)
    except Exception:
        logger.error("DIST alerts analysis failed.")
        raise
    finally:
        if dask_client:
            dask_client.shutdown()

    return result_uris


if __name__ == "__main__":
    main(overwrite=False)
