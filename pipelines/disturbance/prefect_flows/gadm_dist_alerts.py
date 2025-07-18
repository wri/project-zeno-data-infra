import numpy as np

from prefect import flow

from pipelines.disturbance.prefect_flows import dist_common_tasks
from pipelines.globals import DATA_LAKE_BUCKET
from pipelines.disturbance.check_for_new_alerts import s3_object_exists

@flow(name="DIST alerts count")
def dist_alerts_count(dist_zarr_uri: str, dist_version: str, overwrite=False):
    result_filename = "dist_alerts"
    result_key = f"umd_glad_dist_alerts/{dist_version}/tabular/parquet/gadm_{result_filename}.parquet"
    if not overwrite and s3_object_exists(DATA_LAKE_BUCKET, result_key):
        return f"s3://{DATA_LAKE_BUCKET}/{result_key}"

    expected_groups = (
        np.arange(894),  # country ISO codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.arange(731, 1590),  # dates values
        [1, 2, 3],  # confidence values
    )
    datasets = dist_common_tasks.load_data.with_options(name="dist-alerts-load_data")(
        dist_zarr_uri
    )
    compute_input = dist_common_tasks.setup_compute.with_options(
        name="set-up-dist-alerts-compute"
    )(datasets, expected_groups)

    result_dataset = dist_common_tasks.compute_zonal_stat.with_options(
        name="dist-alerts-compute-zonal-stats"
    )(*compute_input)
    result_df = dist_common_tasks.postprocess_result.with_options(
        name="dist-alerts-postprocess-result"
    )(result_dataset)
    result_uri = dist_common_tasks.save_result.with_options(
        name="dist-alerts-save-result"
    )(result_df, dist_version, result_filename)

    return result_uri
