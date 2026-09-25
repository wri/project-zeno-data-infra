"""Full-scale test of the alert-pixel DIST reduction, written to a scratch prefix only.

Throwaway: lives on a benchmark branch only. Runs every DIST sub-flow from one shared
alert-pixel extraction and records timings. Never writes published outputs or
`latest`; compare the scratch outputs with the published ones afterwards.
"""

import json
import time

import fsspec
from prefect import flow
from prefect.logging import get_run_logger

from pipelines.disturbance import prefect_flows, stages, validate_zonal_statistics
from pipelines.disturbance.prefect_flows import dist_common_tasks
from pipelines.disturbance.prefect_flows.dist_flow import (
    create_zarr,
    run_validation_suite,
)

PUBLISHED_PREFIX = dist_common_tasks.DIST_PREFIX
SCRATCH_PREFIX = "s3://lcl-analytics/scratch/dist-alert-pixel-test"

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


@flow(name="DIST alert-pixel full-scale test", log_prints=True)
def dist_alert_pixel_test(
    version=None,
    overwrite=False,
    is_latest=False,
    dist_subflows="all",
    result_prefix=SCRATCH_PREFIX,
) -> list[str]:
    logger = get_run_logger()
    version = version or "v20260919"
    if result_prefix.rstrip("/") == PUBLISHED_PREFIX.rstrip("/"):
        raise ValueError("Refusing to write test outputs to the published DIST prefix")
    names = (
        list(SUBFLOWS)
        if dist_subflows == "all"
        else [n.strip() for n in dist_subflows.split(",")]
    )

    # Sub-flows build their output path from this at call time.
    dist_common_tasks.DIST_PREFIX = result_prefix
    dist_zarr_uri = create_zarr(version, overwrite=False)
    timings, result_uris = {}, []
    try:
        t0 = time.perf_counter()
        n_pixels = stages.load_alert_pixels(dist_zarr_uri)["alert_date"].shape[0]
        timings["extract_alert_pixels_s"] = time.perf_counter() - t0
        logger.info(
            f"Extracted {n_pixels:,} alert pixels in "
            f"{timings['extract_alert_pixels_s'] / 60:.1f} min"
        )

        for name in names:
            subflow, layer = SUBFLOWS[name]
            t0 = time.perf_counter()
            result_uri = subflow(dist_zarr_uri, version, overwrite=True)
            timings[f"{name}_subflow_s"] = time.perf_counter() - t0
            validation = run_validation_suite(
                result_uri, version=version, contextual_layer=layer
            )
            if not validation["validation_passed"]:
                raise ValueError(
                    f"{name} validation failed: {validation.get('details', {})}"
                )
            result_uris.append(result_uri)
            logger.info(
                f"{name}: {timings[f'{name}_subflow_s'] / 60:.1f} min -> {result_uri}"
            )
    finally:
        stages.release_alert_pixels()

    summary = {"version": version, "alert_pixels": n_pixels, "timings_s": timings}
    fs, root = fsspec.core.url_to_fs(result_prefix)
    with fs.open(f"{root}/{version}/results.json", "w") as f:
        json.dump(summary, f, indent=2)
    logger.info(json.dumps(summary, indent=2))
    return result_uris
