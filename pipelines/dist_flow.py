import logging

import coiled
from prefect import flow, task
from prefect.logging import get_run_logger

from pipelines.dist.gadm_dist_alerts import gadm_dist_alerts
from .dist.create_zarr import (
    create_zarr as create_zarr_func,
    get_tiles as get_tiles_func,
)
from .dist.check_for_new_alerts import get_latest_version


DATA_LAKE_BUCKET = "gfw-data-lake"

logging.getLogger("distributed.client").setLevel(logging.ERROR)


@task
def get_new_dist_version() -> str:
    return get_latest_version("umd_glad_dist_alerts")


@task
def get_tiles(dist_version):
    return get_tiles_func("umd_glad_dist_alerts", dist_version)


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
        no_client_timeout="5 seconds"
    )
    cluster.adapt(minimum=10, maximum=50)

    client = cluster.get_client()
    return client, cluster


@task
def create_zarr(dist_version: str, tile_uris, overwrite=False) -> str:
    zarr_uri = create_zarr_func(dist_version, tile_uris, overwrite=overwrite)
    return zarr_uri


@task
def analyze_gadm_dist(dist_zarr_uri, version):

    return gadm_dist_alerts(dist_zarr_uri, version)


@task
def run_validation_suite():
    pass


@flow(name="DIST alerts count")
def main(overwrite=False) -> list[str]:
    client, cluster = (None, None)
    logger = get_run_logger()
    try:
        dist_version = get_new_dist_version()
        tile_uris = get_tiles(dist_version)
        client, cluster = create_cluster()
        dist_zarr_uri = create_zarr(dist_version, tile_uris, overwrite=overwrite)
        result = analyze_gadm_dist(dist_zarr_uri, dist_version)
    except Exception:
        logger.error("DIST alerts analysis failed.")
        raise
    finally:
        if client:
            client.close()

    return [result]


if __name__ == "__main__":
    main(overwrite=False)
