import logging
import os
from enum import Enum

import click
import coiled
from prefect import flow, task
from prefect.logging import get_run_logger

from pipelines.carbon_flux.prefect_flows import carbon_flow
from pipelines.disturbance.prefect_flows import dist_flow
from pipelines.grasslands.prefect_flows import grasslands_flow
from pipelines.natural_lands.prefect_flows import nl_flow as nl_prefect_flow
from pipelines.tree_cover_loss.prefect_flows import tcl_flow

logging.getLogger("distributed.client").setLevel(logging.ERROR)


@task
def create_cluster():
    cluster = coiled.Cluster(
        name="gnw_zonal_stat_count",
        region="us-east-1",
        n_workers=10,
        tags={"project": "gnw_zonal_stat"},
        scheduler_vm_types=["r7g.xlarge"],
        worker_vm_types=["r7g.2xlarge"],
        compute_purchase_option="on-demand",
        no_client_timeout="5 seconds",
        container=os.getenv("PIPELINES_IMAGE"),
        environ={
            "AWS_REQUEST_PAYER": "requester",  # for reading COGS from gfw account
        },
        worker_options={"nthreads": 4},  # to avoid OOMs
    )
    cluster.adapt(minimum=1, maximum=100)

    client = cluster.get_client()
    return client


@flow
def run_dist_update(version=None, overwrite=False, is_latest=False) -> list[str]:
    result_uris = []

    gl_result = grasslands_flow.gadm_grasslands_area(overwrite=overwrite)
    result_uris.append(gl_result)

    nl_result = nl_prefect_flow.gadm_natural_lands_area(overwrite=overwrite)
    result_uris.append(nl_result)

    dist_result = dist_flow.dist_alerts_flow(
        dist_version=version, overwrite=overwrite, is_latest=is_latest
    )
    return [dist_result]


@flow
def run_tcl_update(version, overwrite=False, is_latest=False) -> list[str]:
    result_uris = []

    carbon_result = carbon_flow.gadm_carbon_flux(overwrite=overwrite)
    result_uris.append(carbon_result)

    tcl_result = tcl_flow.umd_tree_cover_loss_flow(version, overwrite=overwrite)
    result_uris.append(tcl_result)

    return result_uris


class UpdateFlow(str, Enum):
    DIST_UPDATE = "dist_update"
    TCL_UPDATE = "tcl_update"


update_flows = {
    UpdateFlow.DIST_UPDATE: run_dist_update,
    UpdateFlow.TCL_UPDATE: run_tcl_update,
}


@flow(
    name="GNW zonal stats update",
    log_prints=True,
    description=(
        "Update the zonal statistics by GADM areas (admin levels 0, 1 and 2). Two flows are available: "
        "-'dist_update' will just run an update on DIST alerts, and is the default for backward compatibility"
        "-'tcl_update' will run tree_cover_loss and carbon_flux flows to provide all necessary updates for TCL."
    ),
)
def run_updates(
    version=None,
    overwrite=False,
    is_latest=False,
    flow_name: UpdateFlow = UpdateFlow.DIST_UPDATE,
) -> list[str]:
    logger = get_run_logger()
    dask_client = None
    result_uris = []

    try:
        dask_client = create_cluster()

        flow_fn = update_flows.get(flow_name)
        if flow_fn is None:
            accepted = [e.value for e in UpdateFlow]
            raise ValueError(
                f"Unsupported flow selection: '{flow_name}'. Accepted values: {accepted}"
            )

        if flow_name == UpdateFlow.TCL_UPDATE and version is None:
            raise ValueError(
                f"version is required when flow is {UpdateFlow.TCL_UPDATE}"
            )

        result_uris = flow_fn(
            version=version,
            overwrite=overwrite,
            is_latest=is_latest,
        )

    except Exception:
        logger.error("Analysis failed.")
        raise
    finally:
        if dask_client:
            dask_client.shutdown()

    return result_uris


@click.command()
@click.option(
    "--flow",
    "flow_name",
    type=click.Choice([e.value for e in UpdateFlow], case_sensitive=False),
    default=UpdateFlow.DIST_UPDATE.value,
    help="Which update flow to run.",
)
@click.option(
    "--version", default=None, help="Dataset version (required for tcl_update)."
)
@click.option("--overwrite", is_flag=True, help="Overwrite existing outputs.")
@click.option("--is-latest", is_flag=True, help="Mark this version as latest.")
def cli(flow_name, version, overwrite, is_latest):
    run_updates(
        version=version,
        overwrite=overwrite,
        is_latest=is_latest,
        flow_name=UpdateFlow(flow_name),
    )


if __name__ == "__main__":
    cli()
