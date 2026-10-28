import logging
import os
import re
from contextlib import nullcontext
from datetime import date
from enum import Enum

import click
import coiled
from dask.distributed import performance_report
from prefect import flow, task
from prefect.logging import get_run_logger
from shapely.geometry import box

from pipelines.aois.prefect_flows import aois_flow
from pipelines.aois.sources import aoi_sources
from pipelines.carbon_flux.prefect_flows import carbon_flow
from pipelines.disturbance.prefect_flows import dist_flow
from pipelines.grasslands.prefect_flows import grasslands_flow
from pipelines.integrated_alerts.prefect_flows import integrated_alerts_flow
from pipelines.land_ghg_inventory.prefect_flows import land_ghg_inventory_flow
from pipelines.natural_lands.prefect_flows import nl_flow as nl_prefect_flow
from pipelines.tree_cover_loss.prefect_flows import tcl_flow
from pipelines.wur_alert_drivers.prefect_flows import wur_flow

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
        idle_timeout="2 hours",
        no_client_timeout="5 minutes",
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
    version, overwrite=False, is_latest=False, bbox=None
) -> list[str]:
    result_uris = []

    result = integrated_alerts_flow.integrated_alerts_zarr_flow(
        version, overwrite=overwrite, is_latest=is_latest, bbox=bbox
    )
    result_uris.append(result)

    return result_uris


# flow_name -> land_ghg_inventory component to run; None means "run all".
LAND_GHG_INVENTORY_COMPONENT = {
    "land_ghg_inventory_update": None,
    "land_ghg_inventory_vegetation_update": "vegetation",
    "land_ghg_inventory_agriculture_update": "agriculture",
    "land_ghg_inventory_mineral_soil_update": "mineral_soil",
    "land_ghg_inventory_organic_soil_update": "organic_soil",
}


@flow
def run_land_ghg_inventory_update(
    version, overwrite=False, is_latest=False, bbox=None, flow_name=None
) -> list[str]:
    """Routes to the land_ghg_inventory component selected by `flow_name` (see
    LAND_GHG_INVENTORY_COMPONENT); runs all components if unset."""
    if flow_name is not None and flow_name not in LAND_GHG_INVENTORY_COMPONENT:
        raise ValueError(
            f"Unsupported land_ghg_inventory flow_name: '{flow_name}'. "
            f"Accepted values: {list(LAND_GHG_INVENTORY_COMPONENT)}"
        )
    return land_ghg_inventory_flow.land_ghg_inventory_area(
        version=version,
        overwrite=overwrite,
        bbox=bbox,
        component=LAND_GHG_INVENTORY_COMPONENT.get(flow_name),
    )


@flow
def run_aois_update(
    version=None, overwrite=False, is_latest=False, aoi_source=None
) -> list[str]:
    return aois_flow.aois_update(aoi_source, overwrite=overwrite)


def run_wur_update(
    version, content_date, overwrite=False, is_latest=False
) -> list[str]:
    # overwrite isn't used: re-running with the same version resumes creating it.
    return wur_flow.wur_alert_drivers_flow(
        content_date=content_date, version=version, is_latest=is_latest
    )


def _parse_bbox(bbox):
    """Parse a 'minx,miny,maxx,maxy' string into a shapely box (or None)."""
    if not bbox:
        return None
    minx, miny, maxx, maxy = (float(v) for v in str(bbox).split(","))
    return box(minx, miny, maxx, maxy)


class UpdateFlow(str, Enum):
    # Not currently used, replaced by INTEGRATED_ALERTS_UPDATE
    DIST_UPDATE = "dist_update"
    # Run by hand yearly for annual TCL update
    TCL_UPDATE = "tcl_update"
    # Run automatically once a week around Sunday
    INTEGRATED_ALERTS_UPDATE = "integrated_alerts_update"
    # LGMS flows run by hand, probably annually
    LAND_GHG_INVENTORY_UPDATE = "land_ghg_inventory_update"
    LAND_GHG_INVENTORY_VEGETATION_UPDATE = "land_ghg_inventory_vegetation_update"
    LAND_GHG_INVENTORY_AGRICULTURE_UPDATE = "land_ghg_inventory_agriculture_update"
    LAND_GHG_INVENTORY_MINERAL_SOIL_UPDATE = "land_ghg_inventory_mineral_soil_update"
    LAND_GHG_INVENTORY_ORGANIC_SOIL_UPDATE = "land_ghg_inventory_organic_soil_update"
    # Run once, until a new AOI dataset version happens
    AOIS_UPDATE = "aois_update"
    # Currently run by hand about once a month when new WUR drivers data is available
    WUR_UPDATE = "wur_update"


update_flows = {
    UpdateFlow.DIST_UPDATE: run_dist_update,
    UpdateFlow.TCL_UPDATE: run_tcl_update,
    UpdateFlow.INTEGRATED_ALERTS_UPDATE: run_integrated_alerts_update,
    UpdateFlow.LAND_GHG_INVENTORY_UPDATE: run_land_ghg_inventory_update,
    UpdateFlow.LAND_GHG_INVENTORY_VEGETATION_UPDATE: run_land_ghg_inventory_update,
    UpdateFlow.LAND_GHG_INVENTORY_AGRICULTURE_UPDATE: run_land_ghg_inventory_update,
    UpdateFlow.LAND_GHG_INVENTORY_MINERAL_SOIL_UPDATE: run_land_ghg_inventory_update,
    UpdateFlow.LAND_GHG_INVENTORY_ORGANIC_SOIL_UPDATE: run_land_ghg_inventory_update,
    UpdateFlow.AOIS_UPDATE: run_aois_update,
    UpdateFlow.WUR_UPDATE: run_wur_update,
}

# flows that produce versioned outputs and therefore require an explicit version
VERSION_REQUIRED_FLOWS = (
    UpdateFlow.TCL_UPDATE,
    UpdateFlow.LAND_GHG_INVENTORY_UPDATE,
    UpdateFlow.LAND_GHG_INVENTORY_VEGETATION_UPDATE,
    UpdateFlow.LAND_GHG_INVENTORY_AGRICULTURE_UPDATE,
    UpdateFlow.LAND_GHG_INVENTORY_MINERAL_SOIL_UPDATE,
    UpdateFlow.LAND_GHG_INVENTORY_ORGANIC_SOIL_UPDATE,
    UpdateFlow.WUR_UPDATE,
)

# flows that don't use Dask, so run on the flow's own machine
CLUSTERLESS_FLOWS = (
    UpdateFlow.AOIS_UPDATE,
    UpdateFlow.WUR_UPDATE,
)

# flow_name values that route into run_land_ghg_inventory_update
LAND_GHG_INVENTORY_FLOWS = (
    UpdateFlow.LAND_GHG_INVENTORY_UPDATE,
    UpdateFlow.LAND_GHG_INVENTORY_VEGETATION_UPDATE,
    UpdateFlow.LAND_GHG_INVENTORY_AGRICULTURE_UPDATE,
    UpdateFlow.LAND_GHG_INVENTORY_MINERAL_SOIL_UPDATE,
    UpdateFlow.LAND_GHG_INVENTORY_ORGANIC_SOIL_UPDATE,
)


def needs_cluster(flow_name: "UpdateFlow", local: bool) -> bool:
    return not local and flow_name not in CLUSTERLESS_FLOWS


def _validate_flow_args(flow_name: "UpdateFlow", version, content_date=None) -> None:
    """Raise if the selected flow needs a version and none was given, or needs a
    content_date (YYYY-MM-DD) and none or an invalid one was given."""
    if flow_name in VERSION_REQUIRED_FLOWS and version is None:
        raise ValueError(f"version is required when flow is {flow_name}")
    if flow_name == UpdateFlow.WUR_UPDATE and not _is_iso_date(content_date):
        raise ValueError(
            f"content_date must be a YYYY-MM-DD date when flow is {flow_name}, "
            f"got {content_date!r}"
        )


def _is_iso_date(value) -> bool:
    """True if value is a real date written exactly as YYYY-MM-DD."""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(value)):
        return False
    try:
        date.fromisoformat(value)
    except ValueError:  # e.g. 2026-02-30
        return False
    return True


@flow(
    name="GNW zonal stats update",
    log_prints=True,
    description=(
        "This is the entry point to run updates via Prefect Cloud UI or CLI to "
        "update zonal statistics for various datasets for GADM areas."
        "Two flows are available currently: "
        "-'dist_update' will just run an update on DIST alerts, and is the "
        "default for backward compatibility"
        "-'tcl_update' will run tree_cover_loss and carbon_flux flows to provide "
        "all necessary updates for TCL."
        "-'wur_update' will create a new Data API version of the WUR alert drivers "
        "dataset (needs version and content_date)."
    ),
)
def run_updates(
    version=None,
    overwrite=False,
    is_latest=False,
    flow_name: UpdateFlow = UpdateFlow.DIST_UPDATE,
    bbox=None,
    aoi_source=None,
    local=False,
    performance_report_path=None,
    content_date=None,
) -> list[str]:
    logger = get_run_logger()
    dask_client = None
    result_uris = []

    # when called from Prefect webhook, the booleans flags are passed as strings,
    # so we need to convert them to booleans
    is_latest = str(is_latest).lower() == "true"
    overwrite = str(overwrite).lower() == "true"
    local = str(local).lower() == "true"
    # bbox (land_ghg_inventory and integrated_alerts flows only) clips the reduce
    # to one area. It is independent of where compute runs: Coiled by default, or
    # local when local=True.
    bbox_geom = _parse_bbox(bbox)

    try:
        if needs_cluster(flow_name, local):
            dask_client = create_cluster()

        flow_fn = update_flows.get(flow_name)
        if flow_fn is None:
            accepted = [e.value for e in UpdateFlow]
            raise ValueError(
                f"Unsupported flow selection: '{flow_name}'. "
                f"Accepted values: {accepted}"
            )

        _validate_flow_args(flow_name, version, content_date)

        # a Dask performance report needs the distributed scheduler, so it only
        # applies to Coiled runs (there is no client under local=True)
        if performance_report_path and dask_client is not None:
            report = performance_report(filename=performance_report_path)
        else:
            if performance_report_path:
                logger.warning("performance report skipped: no cluster (local run)")
            report = nullcontext()

        kwargs = {"version": version, "overwrite": overwrite, "is_latest": is_latest}
        if flow_name in LAND_GHG_INVENTORY_FLOWS:
            kwargs["bbox"] = bbox_geom
            kwargs["flow_name"] = flow_name.value
        if flow_name == UpdateFlow.AOIS_UPDATE:
            kwargs["aoi_source"] = aoi_source
        elif flow_name == UpdateFlow.INTEGRATED_ALERTS_UPDATE:
            kwargs["bbox"] = bbox_geom
        elif flow_name == UpdateFlow.WUR_UPDATE:
            kwargs["content_date"] = content_date
        with report:
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
    "--version",
    default=None,
    help="Dataset version (required for tcl_update and wur_update).",
)
@click.option("--overwrite", is_flag=True, help="Overwrite existing outputs.")
@click.option("--is-latest", is_flag=True, help="Mark this version as latest.")
@click.option(
    "--bbox",
    default=None,
    help=(
        "minx,miny,maxx,maxy to clip the reduce to one area; writes a local "
        "parquet instead of the global S3 path. Only applies to the "
        "land_ghg_inventory flows and integrated_alerts_update."
    ),
)
@click.option(
    "--aoi-source",
    type=click.Choice([source.source for source in aoi_sources()]),
    default=None,
    help="Build only this AOI source (aois_update only); builds all if omitted.",
)
@click.option(
    "--content-date",
    default=None,
    help=(
        "YYYY-MM-DD date up to which the data has been calculated (required for "
        "wur_update)."
    ),
)
@click.option(
    "--local",
    is_flag=True,
    help="Run compute on this machine instead of a Coiled cluster.",
)
@click.option(
    "--performance-report",
    "performance_report_path",
    default=None,
    help="Write a Dask performance report HTML here (Coiled runs only; "
    "ignored with --local).",
)
def cli(
    flow_name,
    version,
    overwrite,
    is_latest,
    bbox,
    aoi_source,
    content_date,
    local,
    performance_report_path,
):
    run_updates(
        version=version,
        overwrite=overwrite,
        is_latest=is_latest,
        flow_name=UpdateFlow(flow_name),
        bbox=bbox,
        aoi_source=aoi_source,
        local=local,
        performance_report_path=performance_report_path,
        content_date=content_date,
    )


if __name__ == "__main__":
    cli()
