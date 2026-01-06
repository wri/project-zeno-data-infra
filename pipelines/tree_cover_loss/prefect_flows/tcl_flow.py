import logging

import numpy as np
import pandas as pd
from prefect import flow

from pipelines.globals import ANALYTICS_BUCKET, tree_cover_loss_zarr_uri, pixel_area_zarr_uri
from pipelines.tree_cover_loss.prefect_flows import tcl_tasks
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists

@flow(name="Tree Cover Loss")
def umd_tree_cover_loss(overwrite: bool = False):
    logging.getLogger("distributed.client").setLevel(logging.ERROR)
    contextual_column_name = "tree_cover_loss_year"
    result_uri = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/admin-tree-cover-loss-2001-2024.parquet"
    funcname = "sum"

    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(1, 25),
        np.arange(999),
        np.arange(1, 86),
        np.arange(1, 854),
    )

    datasets = tcl_tasks.load_data.with_options(
        name="area-by-tcl-load-data"
    )(tree_cover_loss_zarr_uri, pixel_area_uri=pixel_area_zarr_uri)

    compute_input = tcl_tasks.setup_compute.with_options(
        name="set-up-area-by-tcl-compute"
    )(datasets, expected_groups, contextual_name=contextual_column_name)

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="area-by-tcl-compute-zonal-stats"
    )(*compute_input, funcname=funcname)

    print("result_dataset")
    print(result_dataset)

    result_df: pd.DataFrame = common_tasks.postprocess_result.with_options(
        name="area-by-tcl-postprocess-result"
    )(result_dataset)

    # convert to actual years
    result_df[contextual_column_name] = result_df[contextual_column_name] + 2000

    print("result_df")
    print(result_df)


    result_uri = common_tasks.save_result.with_options(
        name="area-by-tcl-save-result"
    )(result_df, result_uri)

    return result_uri
