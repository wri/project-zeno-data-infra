import numpy as np

from prefect import flow

from pipelines.disturbance.prefect_flows import dist_common_tasks
from pipelines.globals import DATA_LAKE_BUCKET
from pipelines.utils import s3_uri_exists
from pipelines.prefect_flows import common_tasks

@flow(name="DIST alerts count by drivers")
def dist_alerts_by_drivers_count(dist_zarr_uri: str, dist_version: str, overwrite=False):
    result_filename = "dist_alerts_by_drivers"
    result_uri = f"s3://{DATA_LAKE_BUCKET}/umd_glad_dist_alerts/{dist_version}/tabular/zonal_stats/gadm/gadm_adm2_{result_filename}.parquet"
    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(894),  # country ISO codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.arange(5),  # driver categories
        np.arange(731, 1590),  # dates values
        [1, 2, 3],  # confidence values
    )
    contextual_uri = f"s3://{DATA_LAKE_BUCKET}/umd_glad_dist_alerts_driver/zarr/umd_dist_alerts_drivers.zarr"
    datasets = common_tasks.load_data.with_options(
        name="dist-alerts-by-natural-lands-load-data"
    )(dist_zarr_uri, contextual_uri=contextual_uri)
    compute_input = dist_common_tasks.setup_compute.with_options(
        name="set-up-dist-alerts-by-drivers-compute"
    )(datasets, expected_groups, contextual_name="driver")

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="dist-alerts-by-drivers-compute-zonal-stats"
    )(*compute_input, funcname="count")
    result_df = common_tasks.postprocess_result.with_options(
        name="dist-alerts-by-drivers-postprocess-result"
    )(result_dataset)
    common_tasks.save_result.with_options(
        name="dist-alerts-by-drivers-save-result"
    )(result_df, result_uri)

    return result_uri
