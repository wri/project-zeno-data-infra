"""Run one DIST sub-flow end to end (reduce, postprocess, save, validate) to scratch.

Throwaway: lives on a benchmark branch only. Never touches published outputs or
`latest`, and skips the grasslands / natural lands flows run_dist_update runs first.
"""

from prefect import flow
from prefect.logging import get_run_logger

from pipelines.disturbance import prefect_flows, validate_zonal_statistics
from pipelines.disturbance.prefect_flows import dist_common_tasks
from pipelines.disturbance.prefect_flows.dist_flow import (
    create_zarr,
    run_validation_suite,
)

SCRATCH_PREFIX = "s3://lcl-analytics/scratch/dist-single-subflow"

SUBFLOWS = {
    "base": (prefect_flows.dist_alerts_area, None),
    "natural_lands": (
        prefect_flows.dist_alerts_by_natural_lands_area,
        validate_zonal_statistics.NATURAL_LANDS,
    ),
    "drivers": (
        prefect_flows.dist_alerts_by_drivers_area,
        validate_zonal_statistics.DIST_DRIVERS,
    ),
    "grasslands": (
        prefect_flows.dist_alerts_by_grasslands_area,
        validate_zonal_statistics.GRASSLANDS,
    ),
    "land_cover": (
        prefect_flows.dist_alerts_by_land_cover_area,
        validate_zonal_statistics.LAND_COVER,
    ),
}


@flow(name="DIST single sub-flow benchmark", log_prints=True)
def dist_single_subflow(
    version=None,
    overwrite=False,
    is_latest=False,
    dist_subflow="natural_lands",
    result_prefix=SCRATCH_PREFIX,
) -> list[str]:
    if version is None:
        raise ValueError("version is required, e.g. v20260919")
    if dist_subflow not in SUBFLOWS:
        raise ValueError(f"dist_subflow must be one of {list(SUBFLOWS)}")
    logger = get_run_logger()

    # Sub-flows build their output path from this at call time.
    dist_common_tasks.DIST_PREFIX = result_prefix
    subflow_fn, contextual_layer = SUBFLOWS[dist_subflow]

    dist_zarr_uri = create_zarr(version, overwrite=False)
    result_uri = subflow_fn(dist_zarr_uri, version, overwrite=True)
    logger.info(f"{dist_subflow} result written to {result_uri}")

    validation = run_validation_suite(
        result_uri, version=version, contextual_layer=contextual_layer
    )
    if not validation["validation_passed"]:
        raise ValueError(f"Validation failed: {validation.get('details', {})}")
    return [result_uri]
