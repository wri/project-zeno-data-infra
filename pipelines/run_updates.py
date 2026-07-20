import logging
import os
from enum import Enum

import click
import coiled
from prefect import flow, task
from prefect.logging import get_run_logger
from shapely.geometry import box

from pipelines.afolu.prefect_flows import afolu_flow
from pipelines.carbon_flux.prefect_flows import carbon_flow
from pipelines.disturbance.prefect_flows import dist_flow
from pipelines.grasslands.prefect_flows import grasslands_flow
from pipelines.integrated_alerts.prefect_flows import integrated_alerts_flow
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

    carbon_result = carbon_flow.gadm_carbon_flux(version, overwrite=overwrite)
    result_uris.append(carbon_result)

    tcl_result = tcl_flow.umd_tree_cover_loss_flow(version, overwrite=overwrite)
    result_uris.append(tcl_result)

    return result_uris


@flow
def run_integrated_alerts_update(
    version, overwrite=False, is_latest=False
) -> list[str]:
    result_uris = []

    result = integrated_alerts_flow.integrated_alerts_zarr_flow(
        version, overwrite=overwrite, is_latest=is_latest
    )
    result_uris.append(result)

    return result_uris


@flow
def run_afolu_update(version, overwrite=False, is_latest=False, bbox=None) -> list[str]:
    return [afolu_flow.afolu_area(version=version, overwrite=overwrite, bbox=bbox)]


def _parse_bbox(bbox):
    """Parse a 'minx,miny,maxx,maxy' string into a shapely box (or None)."""
    if not bbox:
        return None
    minx, miny, maxx, maxy = (float(v) for v in str(bbox).split(","))
    return box(minx, miny, maxx, maxy)


class UpdateFlow(str, Enum):
    DIST_UPDATE = "dist_update"
    TCL_UPDATE = "tcl_update"
    INTEGRATED_ALERTS_UPDATE = "integrated_alerts_update"
    AFOLU_UPDATE = "afolu_update"


update_flows = {
    UpdateFlow.DIST_UPDATE: run_dist_update,
    UpdateFlow.TCL_UPDATE: run_tcl_update,
    UpdateFlow.INTEGRATED_ALERTS_UPDATE: run_integrated_alerts_update,
    UpdateFlow.AFOLU_UPDATE: run_afolu_update,
}


@flow(
    name="GNW zonal stats update",
    log_prints=True,
    description=(
        "This is the entry point to run updates via Prefect Cloud UI or CLI to update zonal statistics for various datasets for GADM areas."
        "Two flows are available currently: "
        "-'dist_update' will just run an update on DIST alerts, and is the default for backward compatibility"
        "-'tcl_update' will run tree_cover_loss and carbon_flux flows to provide all necessary updates for TCL."
    ),
)
def run_updates(
    version=None,
    overwrite=False,
    is_latest=False,
    flow_name: UpdateFlow = UpdateFlow.DIST_UPDATE,
    bbox=None,
    local=False,
) -> list[str]:
    logger = get_run_logger()
    dask_client = None
    result_uris = []

    # when called from Prefect webhook, the booleans flags are passed as strings, so we need to convert them to booleans
    is_latest = str(is_latest).lower() == "true"
    overwrite = str(overwrite).lower() == "true"
    local = str(local).lower() == "true"
    # bbox (afolu_update only) clips the reduce to one area; it is independent of
    # where compute runs -- Coiled by default, or the local machine with local=True.
    bbox_geom = _parse_bbox(bbox)

    try:
        if not local:
            dask_client = create_cluster()

        flow_fn = update_flows.get(flow_name)
        if flow_fn is None:
            accepted = [e.value for e in UpdateFlow]
            raise ValueError(
                f"Unsupported flow selection: '{flow_name}'. Accepted values: {accepted}"
            )

        if (
            flow_name in (UpdateFlow.TCL_UPDATE, UpdateFlow.AFOLU_UPDATE)
            and version is None
        ):
            raise ValueError(f"version is required when flow is {flow_name}")

        kwargs = dict(version=version, overwrite=overwrite, is_latest=is_latest)
        if flow_name == UpdateFlow.AFOLU_UPDATE:
            kwargs["bbox"] = bbox_geom
        result_uris = flow_fn(**kwargs)

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
@click.option(
    "--bbox",
    default=None,
    help=(
        "minx,miny,maxx,maxy to clip afolu_update to one area; writes a local "
        "parquet instead of the global S3 path. afolu_update only."
    ),
)
@click.option(
    "--local",
    is_flag=True,
    help="Run compute on this machine instead of a Coiled cluster.",
)
def cli(flow_name, version, overwrite, is_latest, bbox, local):
    run_updates(
        version=version,
        overwrite=overwrite,
        is_latest=is_latest,
        flow_name=UpdateFlow(flow_name),
        bbox=bbox,
        local=local,
    )


if __name__ == "__main__":
    cli()
