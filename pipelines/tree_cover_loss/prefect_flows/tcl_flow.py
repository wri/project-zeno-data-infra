import logging

import numpy as np
import pandas as pd
from prefect import flow

from pipelines.globals import (
    ANALYTICS_BUCKET,
    tree_cover_loss_zarr_uri,
    pixel_area_zarr_uri,
    carbon_emissions_zarr_uri,
    tree_cover_density_zarr_uri,
)
from pipelines.tree_cover_loss.prefect_flows import tcl_tasks
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists

# Threshold mapping for post-processing
thresh_to_pct = {
    1: '10',
    2: '15',
    3: '20',
    4: '25',
    5: '30',
    6: '50',
    7: '75',
}

@flow(name="Tree Cover Loss")
def umd_tree_cover_loss(overwrite: bool = False):
    logging.getLogger("distributed.client").setLevel(logging.ERROR)
    contextual_column_name = "tree_cover_loss_year"
    result_uri = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/admin-tree-cover-loss-emissions-2001-2024.parquet"
    funcname = "sum"

    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(1, 25),
        np.arange(1, 8),
        np.arange(999),
        np.arange(1, 86),
        np.arange(1, 854),
    )

    datasets = tcl_tasks.load_data.with_options(
        name="area-emissions-by-tcl-load-data"
    )(
        tree_cover_loss_zarr_uri,
        pixel_area_uri=pixel_area_zarr_uri,
        carbon_emissions_uri=carbon_emissions_zarr_uri,
        tree_cover_density_uri=tree_cover_density_zarr_uri,
    )

    compute_input = tcl_tasks.setup_compute.with_options(
        name="set-up-area-emissions-by-tcl-compute"
    )(datasets, expected_groups, contextual_name=contextual_column_name)

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="area-emissions-by-tcl-compute-zonal-stats"
    )(*compute_input, funcname=funcname)

    print("result_dataset")
    print(result_dataset)

    # Use custom postprocessing for multi-variable Dataset
    result_df: pd.DataFrame = tcl_tasks.postprocess_result_multi_var.with_options(
        name="area-emissions-by-tcl-postprocess-result"
    )(result_dataset)

    # convert year values (1-24) to actual years (2001-2024)
    result_df[contextual_column_name] = result_df[contextual_column_name] + 2000

    # convert tcl thresholds to percentages
    result_df['threshold'] = result_df['threshold'].map(thresh_to_pct)

    # convert country codes
    from pipelines.prefect_flows.common_stages import numeric_to_alpha3
    result_df['country'] = result_df['country'].map(numeric_to_alpha3)
    result_df.dropna(subset=['country'], inplace=True)

    print("result_df")
    print(result_df)

    result_uri = common_tasks.save_result.with_options(
        name="area-emissions-by-tcl-save-result"
    )(result_df, result_uri)

    return result_uri
