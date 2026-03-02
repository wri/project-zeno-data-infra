import logging
import os

import coiled
from prefect import flow, task
from prefect.logging import get_run_logger

from pipelines.carbon_flux.prefect_flows import carbon_flow
from pipelines.tree_cover_loss.prefect_flows import tcl_flow

logging.getLogger("distributed.client").setLevel(logging.ERROR)


@task
def create_cluster():
    cluster = coiled.Cluster(
        name="gnw_tcl_update",
        region="us-east-1",
        n_workers=10,
        tags={"project": "gnw_zonal_stat"},
        scheduler_vm_types=["r7g.xlarge"],
        worker_vm_types=["r7g.2xlarge"],
        compute_purchase_option="spot_with_fallback",
        no_client_timeout="5 seconds",
        container=os.getenv("PIPELINES_IMAGE"),
        environ={
            "AWS_REQUEST_PAYER": "requester",  # for reading COGS from gfw account
        },
    )
    cluster.adapt(minimum=10, maximum=75)

    client = cluster.get_client()
    return client


@flow(
    name="GNW TCL update",
    log_prints=True,
    description=(
        "Update the zonal statistics by GADM areas (admin levels 0, 1 and 2) "
        "for all TCL-related GNW datasets, including TCL and carbon flux."
    ),
)
def run_tcl_update(
    version: str,
    overwrite=False,
) -> list[str]:
    logger = get_run_logger()
    dask_client = None
    result_uris = []
    try:
        dask_client = create_cluster()

        tcl_result = tcl_flow.umd_tree_cover_loss_flow(version, overwrite=overwrite)
        result_uris.append(tcl_result)

        carbon_result = carbon_flow.gadm_carbon_flux(overwrite=overwrite)
        result_uris.append(carbon_result)

    except Exception:
        logger.error("Analysis failed.")
        raise
    finally:
        if dask_client:
            dask_client.shutdown()

    return result_uris


def main(version, overwrite=False, is_latest=False):
    run_tcl_update(version=version, overwrite=overwrite, is_latest=is_latest)


if __name__ == "__main__":
    main("v1.12", overwrite=False)
