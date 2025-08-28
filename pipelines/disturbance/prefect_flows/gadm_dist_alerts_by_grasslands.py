import numpy as np
import pandas as pd

from prefect import flow

from pipelines.disturbance.prefect_flows import dist_common_tasks
from pipelines.globals import DATA_LAKE_BUCKET
from pipelines.utils import s3_uri_exists
from pipelines.prefect_flows import common_tasks


@flow(name="DIST alerts area by grasslands")
def dist_alerts_by_grasslands_area(dist_zarr_uri: str, dist_version: str, overwrite=False):
    result_filename = "dist_alerts_by_grasslands"
    result_uri = f"s3://{DATA_LAKE_BUCKET}/umd_glad_dist_alerts/{dist_version}/tabular/zonal_stats/gadm/gadm_adm2_{result_filename}.parquet"
    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(999),  # country ISO codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        [0, 1],          # grasslands boolean
        np.arange(731, 2000),  # dates values
        [1, 2, 3],  # confidence values
    )
    contextual_uri = "s3://gfw-data-lake/gfw_grasslands/v1/zarr/natural_grasslands_4kchunk.zarr/"
    datasets = dist_common_tasks.load_data.with_options(
        name="dist-alerts-by-grasslands-load-data"
    )(dist_zarr_uri, contextual_uri=contextual_uri)
    # We only need year 2022 of the grasslands contextual layer. We can fix later to
    # put this in a grasslands-specific setup_compute() task.
    datasets = datasets[:5] + (datasets[5].sel(year=2022), )
    compute_input = dist_common_tasks.setup_compute.with_options(
        name="set-up-dist-alerts-by-grasslands-compute"
    )(datasets, expected_groups, contextual_name="grasslands")

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="dist-alerts-by-grasslands-compute-zonal-stats"
    )(*compute_input, funcname="sum")
    result_df: pd.DataFrame = dist_common_tasks.postprocess_result.with_options(
        name="dist-alerts-by-grasslands-postprocess-result"
    )(result_dataset)

    result_df["grasslands"] = result_df["grasslands"].apply(
        (lambda x: "grasslands" if x == 1 else "non-grasslands")
    )

    result_uri = common_tasks.save_result.with_options(
        name="dist-alerts-by-grasslands-save-result"
    )(result_df, result_uri)

    return result_uri
