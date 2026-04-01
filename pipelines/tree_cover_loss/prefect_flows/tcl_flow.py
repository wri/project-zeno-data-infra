import logging
from typing import Optional, Tuple

import numpy as np
from prefect import flow
from shapely.geometry import box

from pipelines.carbon_flux.stages import DATASETS as CARBON_FLUX_DATASETS
from pipelines.globals import (
    ANALYTICS_BUCKET,
    ifl_intact_forest_lands_zarr_uri,
    pixel_area_zarr_uri,
    sbtn_natural_forests_zarr_uri,
    tree_cover_density_zarr_uri,
    umd_primary_forests_zarr_uri,
)
from pipelines.prefect_flows import common_tasks
from pipelines.tree_cover_loss.prefect_flows import tcl_tasks
from pipelines.utils import s3_uri_exists


@flow(name="Tree Cover Loss")
def umd_tree_cover_loss_flow(
    version: str,
    overwrite=False,
    bbox: Optional[Tuple[float, float, float, float]] = None,
):
    """Run the UMD tree cover loss flow and persist the resulting parquet.

    Args:
        version: Output version string used in the destination S3 path.
        overwrite: If True, recompute and overwrite existing output at the target URI.
        bbox: Optional bounding box as ``(min_x, min_y, max_x, max_y)`` to limit
            processing to a spatial subset.

    Returns:
        The S3 URI for the saved parquet result.
    """
    result_uri = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/tcl/{version}/admin-tree-cover-loss.parquet"

    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    logging.getLogger("distributed.client").setLevel(logging.ERROR)

    tcl_zarr_uris = tcl_tasks.create_zarrs.with_options(name="create-tcl-zarrs")(
        overwrite=overwrite
    )

    if bbox is not None:
        bbox = box(*bbox)

    expected_groups = (
        np.arange(1, 25),  # tcl years
        np.arange(0, 8),  # tcd threshold
        np.arange(0, 2),  # ifl
        np.arange(0, 8),  # drivers
        np.arange(0, 2),  # primary_forests
        np.arange(0, 3),  # natural forest class (0=unknown, 1=natural, 2=non-natural)
        np.arange(999),  # countries
        np.arange(86),  # adm1s
        np.arange(854),  # adm2s
    )

    datasets = tcl_tasks.load_data.with_options(name="area-emissions-by-tcl-load-data")(
        tcl_zarr_uris["tree_cover_loss"],
        pixel_area_uri=pixel_area_zarr_uri,
        carbon_emissions_uri=CARBON_FLUX_DATASETS["carbon_gross_emissions"]["zarr_uri"],
        tree_cover_density_uri=tree_cover_density_zarr_uri,
        ifl_uri=ifl_intact_forest_lands_zarr_uri,
        drivers_uri=tcl_zarr_uris["drivers"],
        primary_forests_uri=umd_primary_forests_zarr_uri,
        natural_forests_uri=sbtn_natural_forests_zarr_uri,
        tree_cover_loss_from_fires_uri=tcl_zarr_uris["tree_cover_loss_from_fires"],
        bbox=bbox,
        group="pipeline",
    )

    compute_input = tcl_tasks.setup_compute.with_options(
        name="set-up-area-emissions-by-tcl-compute"
    )(datasets, expected_groups)

    result = common_tasks.compute_zonal_stat.with_options(
        name="area-emissions-by-tcl-compute-zonal-stats"
    )(*compute_input, funcname="sum")

    result_df = tcl_tasks.postprocess_result.with_options(
        name="area-emissions-by-tcl-postprocess-result"
    )(result)

    result_uri = common_tasks.save_result.with_options(
        name="area-emissions-by-tcl-save-result"
    )(result_df, result_uri)

    if not tcl_tasks.qc_against_validation_source.with_options(
        name="area-emissions-by-tcl-qc-validation"
    )(result_df=result_df, version=version):
        raise AssertionError("TCL did not pass QC validation, stopping job")

    return result_uri
