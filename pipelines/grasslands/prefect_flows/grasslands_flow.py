import logging

import numpy as np
import pandas as pd
from prefect import flow

from pipelines.globals import ANALYTICS_BUCKET
from pipelines.grasslands.prefect_flows import grasslands_tasks
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists


@flow(name="Natural grasslands area")
def gadm_grasslands_area(overwrite: bool = False):
    logging.getLogger("distributed.client").setLevel(logging.ERROR)  # or logging.ERROR

    base_uri = "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area_ha.zarr"
    contextual_uri = (
        "s3://gfw-data-lake/gfw_grasslands/v1/zarr/natural_grasslands_4kchunk.zarr/"
    )
    contextual_column_name = "grasslands"
    result_uri = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/admin-grasslands.parquet"
    funcname = "sum"

    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(999),  # country iso codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
    )

    datasets = grasslands_tasks.load_data.with_options(
        name="area-by-grasslands-load-data"
    )(base_uri, contextual_uri=contextual_uri)

    compute_input = grasslands_tasks.setup_compute.with_options(
        name="set-up-area-by-grasslands-compute"
    )(datasets, expected_groups, contextual_name=contextual_column_name)

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="area-by-grasslands-compute-zonal-stats"
    )(*compute_input, funcname=funcname)

    print("result_dataset")
    print(result_dataset)

    result_df: pd.DataFrame = common_tasks.postprocess_result.with_options(
        name="area-by-grasslands-postprocess-result"
    )(result_dataset)

    print("result_df")
    print(result_df)
    result_df.rename(columns={"value": "area_ha"}, inplace=True)
    result_df["area_ha"] = result_df["area_ha"] / 1e4

    result_uri = common_tasks.save_result.with_options(
        name="area-by-grasslands-save-result"
    )(result_df, result_uri)

    return result_uri
