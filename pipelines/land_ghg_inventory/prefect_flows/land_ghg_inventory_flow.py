import logging
from typing import Optional

import numpy as np
from prefect import flow
from shapely.geometry import Polygon

from pipelines.globals import (
    ANALYTICS_BUCKET,
    country_zarr_uri,
    gadm_country_code_count,
    gadm_region_code_count,
    gadm_subregion_code_count,
    land_ghg_inventory_vegetation_zarr_uri,
    pixel_area_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.land_ghg_inventory.prefect_flows import land_ghg_inventory_tasks
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists

# Pipeline-specific reduce group axes (the admin axes come from globals). flox
# silently drops labels at or above a bound, so these must cover the real range.
VEGETATION_CATEGORY_COUNT = 5  # 0=excluded .. 4=non_trees_remaining_non_trees
YEAR_COUNT = 9  # year index 0..8 -> 2016..2024


@flow(name="Land GHG inventory vegetation", retries=2, retry_delay_seconds=120)
def land_ghg_inventory_vegetation(
    version: str,
    overwrite: bool = False,
    bbox: Optional[Polygon] = None,
) -> str:
    """Land GHG inventory vegetation land-flux zonal stats: gross emissions /
    removals / net flux and area, grouped by ``land_state_class`` (tree loss /
    tree gain / trees-remaining / non-trees-remaining) x year, rolled up to aoi_id.

    ``bbox`` clips the reduce to one area for a laptop-friendly local run; the
    result is written to a local parquet
    (``admin-land_ghg_inventory-vegetation-{version}.parquet``) instead of the
    canonical global S3 path (which would be a partial write)."""
    if bbox is None:
        result_uri = (
            f"s3://{ANALYTICS_BUCKET}/zonal-statistics/land_ghg_inventory-vegetation/"
            f"{version}/admin-land_ghg_inventory-vegetation.parquet"
        )
        if not overwrite and s3_uri_exists(result_uri):
            return result_uri
    else:
        # local bbox test run
        result_uri = f"admin-land_ghg_inventory-vegetation-{version}.parquet"

    expected_groups = (
        np.arange(gadm_country_code_count),
        np.arange(gadm_region_code_count),
        np.arange(gadm_subregion_code_count),
        np.arange(VEGETATION_CATEGORY_COUNT),
        np.arange(YEAR_COUNT),
    )
    datasets = land_ghg_inventory_tasks.load_vegetation.with_options(
        name="land_ghg_inventory-vegetation-load-data"
    )(
        land_ghg_inventory_vegetation_zarr_uri,
        pixel_area_zarr_uri,
        country_zarr_uri,
        region_zarr_uri,
        subregion_zarr_uri,
        bbox,
    )
    compute_input = land_ghg_inventory_tasks.setup_vegetation_compute.with_options(
        name="set-up-land_ghg_inventory-vegetation-compute"
    )(datasets, expected_groups)
    reduced = common_tasks.compute_zonal_stat.with_options(
        name="land_ghg_inventory-vegetation-compute-zonal-stats"
    )(*compute_input, funcname="sum")
    result_df = land_ghg_inventory_tasks.vegetation_result_dataframe.with_options(
        name="land_ghg_inventory-vegetation-postprocess-result"
    )(reduced)
    return common_tasks.save_result.with_options(
        name="land_ghg_inventory-vegetation-save-result"
    )(result_df, result_uri)


@flow(name="Land GHG inventory agriculture", retries=2, retry_delay_seconds=120)
def land_ghg_inventory_agriculture(
    version: str,
    overwrite: bool = False,
    bbox: Optional[Polygon] = None,
) -> str:
    """Land GHG inventory agriculture zonal stats: cropland + livestock emissions,
    admin-only (no land_state_class, no year axis -- a single static snapshot),
    rolled up to aoi_id.

    ``bbox`` clips the reduce to one area for a laptop-friendly local run; the
    result is written to a local parquet
    (``admin-land_ghg_inventory-agriculture-{version}.parquet``) instead of the
    canonical global S3 path (which would be a partial write)."""
    if bbox is None:
        result_uri = (
            f"s3://{ANALYTICS_BUCKET}/zonal-statistics/land_ghg_inventory-agriculture/"
            f"{version}/admin-land_ghg_inventory-agriculture.parquet"
        )
        if not overwrite and s3_uri_exists(result_uri):
            return result_uri
    else:
        # local bbox test run
        result_uri = f"admin-land_ghg_inventory-agriculture-{version}.parquet"

    expected_groups = (
        np.arange(gadm_country_code_count),
        np.arange(gadm_region_code_count),
        np.arange(gadm_subregion_code_count),
    )
    agriculture_zarr_uri = (
        land_ghg_inventory_tasks.prepare_agriculture_zarr.with_options(
            name="land_ghg_inventory-agriculture-resample-source-zarr"
        )(overwrite=overwrite)
    )
    datasets = land_ghg_inventory_tasks.load_agriculture.with_options(
        name="land_ghg_inventory-agriculture-load-data"
    )(
        agriculture_zarr_uri,
        country_zarr_uri,
        region_zarr_uri,
        subregion_zarr_uri,
        bbox,
    )
    compute_input = land_ghg_inventory_tasks.setup_agriculture_compute.with_options(
        name="set-up-land_ghg_inventory-agriculture-compute"
    )(datasets, expected_groups)
    reduced = common_tasks.compute_zonal_stat.with_options(
        name="land_ghg_inventory-agriculture-compute-zonal-stats"
    )(*compute_input, funcname="sum")
    result_df = land_ghg_inventory_tasks.agriculture_result_dataframe.with_options(
        name="land_ghg_inventory-agriculture-postprocess-result"
    )(reduced)
    return common_tasks.save_result.with_options(
        name="land_ghg_inventory-agriculture-save-result"
    )(result_df, result_uri)


# component name -> its subflow. Add new components (e.g. soil) here to expose
# them through `component`, without touching run_updates.py.
COMPONENT_FLOWS = {
    "vegetation": land_ghg_inventory_vegetation,
    "agriculture": land_ghg_inventory_agriculture,
}
ALL_COMPONENTS = tuple(COMPONENT_FLOWS)


@flow(name="Land GHG inventory area", retries=2, retry_delay_seconds=120)
def land_ghg_inventory_area(
    version: str,
    overwrite: bool = False,
    bbox: Optional[Polygon] = None,
    component: Optional[str] = None,
) -> list[str]:
    """Land GHG inventory zonal stats, parent flow dispatching to the pipeline's
    per-component subflows: vegetation (``land_ghg_inventory_vegetation``) and
    agriculture (``land_ghg_inventory_agriculture``), each saved to its own
    parquet. Results are QC'd out-of-band against the reference dataset (see the
    QC notebook), not in-flow.

    ``component`` selects a single subflow to run, e.g. ``"agriculture"``; unset
    (the default) runs all components. An unknown name raises immediately."""
    logging.getLogger("distributed.client").setLevel(logging.ERROR)

    if component is not None:
        if component not in COMPONENT_FLOWS:
            raise ValueError(
                f"Unknown land_ghg_inventory component: '{component}'. "
                f"Accepted: {ALL_COMPONENTS}"
            )
        return [
            COMPONENT_FLOWS[component](version=version, overwrite=overwrite, bbox=bbox)
        ]

    return [
        flow_fn(version=version, overwrite=overwrite, bbox=bbox)
        for flow_fn in COMPONENT_FLOWS.values()
    ]
