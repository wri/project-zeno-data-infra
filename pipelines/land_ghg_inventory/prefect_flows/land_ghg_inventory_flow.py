import logging
from typing import Optional

import numpy as np
from prefect import flow
from shapely.geometry import Polygon

from pipelines.globals import (
    ANALYTICS_BUCKET,
    country_zarr_uri,
    land_ghg_inventory_vegetation_zarr_uri,
    pixel_area_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.land_ghg_inventory.prefect_flows import land_ghg_inventory_tasks
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists


def _vegetation_result_df(bbox=None):
    """Run the vegetation reduce and return its tidy component rows."""
    expected_groups = (
        np.arange(999),  # country iso codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.array([0, 1, 2, 3, 4]),  # vegetation category codes
        np.arange(9),  # year index 0..8 -> 2016..2024
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
    return land_ghg_inventory_tasks.vegetation_result_dataframe.with_options(
        name="land_ghg_inventory-vegetation-postprocess-result"
    )(reduced)


@flow(name="Land GHG inventory vegetation area", retries=2, retry_delay_seconds=120)
def land_ghg_inventory_area(
    version: str,
    overwrite: bool = False,
    bbox: Optional[Polygon] = None,
):
    """Land GHG inventory vegetation land-flux zonal stats: gross emissions /
    removals / net flux and area, grouped by ``land_state_class`` (tree loss /
    tree gain / trees-remaining / non-trees-remaining) x year, rolled up to aoi_id.
    Soil is a separate pipeline with its own output parquet. Results are QC'd
    out-of-band against the reference dataset (see the QC notebook), not in-flow.

    ``bbox`` clips the reduce to one area for a laptop-friendly local run; the
    result is written to a local parquet
    (``admin-land_ghg_inventory-vegetation-{version}.parquet``) instead of the
    canonical global S3 path (which would be a partial write)."""
    logging.getLogger("distributed.client").setLevel(logging.ERROR)

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

    result_df = _vegetation_result_df(bbox)

    return common_tasks.save_result.with_options(name="land_ghg_inventory-save-result")(
        result_df, result_uri
    )
