import logging
import os

import coiled
from prefect import flow, task
from prefect.logging import get_run_logger

from pipelines.carbon_flux.prefect_flows import carbon_flow
from pipelines.disturbance.prefect_flows import dist_flow
from pipelines.grasslands.prefect_flows import grasslands_flow
from pipelines.natural_lands.prefect_flows import nl_flow as nl_prefect_flow

logging.getLogger("distributed.client").setLevel(logging.ERROR)
import dask


@task
def create_cluster():
    cluster = coiled.Cluster(
        name="gnw_zonal_stat_count",
        region="us-east-1",
        n_workers=50,
        tags={"project": "gnw_zonal_stat"},
        scheduler_vm_types=["r7g.xlarge"],
        worker_vm_types=["r7g.2xlarge"],
        compute_purchase_option="spot_with_fallback",
        no_client_timeout="600 seconds",
        container=os.getenv("PIPELINES_IMAGE"),
        environ={
            "AWS_REQUEST_PAYER": "requester",  # for reading COGS from gfw account
        },
    )
    dask.config.set({"distributed.scheduler.allowed-failures": 10})
    cluster.adapt(minimum=50, maximum=50)

    client = cluster.get_client()
    return client


@flow(
    name="GNW zonal stats update",
    log_prints=True,
    description=(
        "Update the zonal statistics by GADM areas (admin levels 0, 1 and 2) "
        "for all GNW datasets, currently including DIST Alerts, Natural Lands, "
        "Grasslands, and Carbon Flux."
    ),
)
def run_updates(dist_version=None, overwrite=False, is_latest=False) -> list[str]:
    logger = get_run_logger()
    dask_client = None
    result_uris = []
    try:
        dask_client = create_cluster()
        # gl_result = grasslands_flow.gadm_grasslands_area(overwrite=overwrite)
        # result_uris.append(gl_result)

        # nl_result = nl_prefect_flow.gadm_natural_lands_area(overwrite=overwrite)
        # result_uris.append(nl_result)

        # dist_result = dist_flow.dist_alerts_flow(
        #     dist_version=dist_version, overwrite=overwrite, is_latest=is_latest
        # )
        # result_uris.append(dist_result)

        carbon_result = carbon_flow.gadm_carbon_flux(overwrite=overwrite)
        result_uris.append(carbon_result)

    except Exception:
        logger.error("Analysis failed.")
        raise
    finally:
        if dask_client:
            dask_client.shutdown()

    return result_uris


def main(dist_version=None, overwrite=False, is_latest=False):
    run_updates(dist_version=dist_version, overwrite=overwrite, is_latest=is_latest)


if __name__ == "__main__":
    main(overwrite=False)
