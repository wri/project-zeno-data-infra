import logging

import numpy as np
from prefect import flow

from pipelines.globals import (
    ANALYTICS_BUCKET,
    pixel_area_zarr_uri,
    sbtn_natural_lands_zarr_uri,
)
from pipelines.natural_lands.prefect_flows import nl_tasks
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists


@flow(name="Natural lands area")
def gadm_natural_lands_area(overwrite: bool = False):
    logging.getLogger("distributed.client").setLevel(logging.ERROR)  # or logging.ERROR

    contextual_column_name = "natural_lands"
    result_uri = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/admin-natural-lands.parquet"
    funcname = "sum"

    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(999),  # country iso codes
        np.arange(1, 86),  # region codes
        np.arange(1, 854),  # subregion codes
        np.arange(1, 22),  # natural lands categories
    )

    datasets = common_tasks.load_data.with_options(
        name="area-by-natural-lands-load-data"
    )(pixel_area_zarr_uri, contextual_uri=sbtn_natural_lands_zarr_uri)

    compute_input = nl_tasks.setup_compute.with_options(
        name="set-up-area-by-natural-lands-compute"
    )(datasets, expected_groups, contextual_name=contextual_column_name)

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="area-by-natural-lands-compute-zonal-stats"
    )(*compute_input, funcname=funcname)
    result_df = nl_tasks.postprocess_result.with_options(
        name="area-by-natural-lands-postprocess-result"
    )(result_dataset)
    result_uri = common_tasks.save_result.with_options(
        name="area-by-natural-lands-save-result"
    )(result_df, result_uri)
    return result_uri
