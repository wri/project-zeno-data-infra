import logging
from typing import Optional

import numpy as np
import pandas as pd
from prefect import flow
from shapely.geometry import Polygon

from pipelines.globals import (
    ANALYTICS_BUCKET,
    carbon_emissions_zarr_uri,
    ifl_intact_forest_lands_zarr_uri,
    pixel_area_zarr_uri,
    tree_cover_density_zarr_uri,
    tree_cover_loss_zarr_uri,
    umd_primary_forests_zarr_uri,
    wri_google_1km_drivers_zarr_uri,
)
from pipelines.prefect_flows import common_tasks
from pipelines.tree_cover_loss.prefect_flows import tcl_tasks
from pipelines.tree_cover_loss.stages import TreeCoverLossTasks
from pipelines.utils import s3_uri_exists


@flow
def umd_tree_cover_loss_flow(overwrite=False):
    result_uri = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/admin-tree-cover-loss-emissions-2001-2024.parquet"

    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    result_df = umd_tree_cover_loss(task=tcl_tasks.TreeCoverLossPrefectTasks)

    result_uri = common_tasks.save_result.with_options(
        name="area-emissions-by-tcl-save-result"
    )(result_df, result_uri)

    return result_uri


def main(overwrite=False):
    umd_tree_cover_loss_flow(overwrite=overwrite)


if __name__ == "__main__":
    main(overwrite=False)
