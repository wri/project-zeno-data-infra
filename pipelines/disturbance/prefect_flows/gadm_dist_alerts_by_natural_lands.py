import numpy as np

from prefect import flow

from pipelines.disturbance.prefect_flows import dist_common_tasks
from pipelines.globals import DATA_LAKE_BUCKET
from pipelines.utils import s3_uri_exists
from pipelines.prefect_flows import common_tasks

@flow(name="DIST alerts count by natural lands")
def dist_alerts_by_natural_lands_count(dist_zarr_uri: str, dist_version: str, overwrite=False):
    result_filename = "dist_alerts_by_natural_lands"
    result_uri = f"s3://{DATA_LAKE_BUCKET}/umd_glad_dist_alerts/{dist_version}/tabular/zonal_stats/gadm/gadm_adm2_{result_filename}.parquet"
    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(894),  # country ISO codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.arange(22),  # natural lands categories
        np.arange(731, 1590),  # dates values
        [1, 2, 3],  # confidence values
    )
    contextual_uri = f"s3://{DATA_LAKE_BUCKET}/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes.zarr"
    datasets = common_tasks.load_data.with_options(
        name="dist-alerts-by-natural-lands-load-data"
    )(dist_zarr_uri, contextual_uri=contextual_uri)
    compute_input = dist_common_tasks.setup_compute.with_options(
        name="set-up-dist-alerts-by-natural-lands-compute"
    )(datasets, expected_groups, contextual_name="natural_lands")

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="dist-alerts-by-natural-lands-compute-zonal-stats"
    )(*compute_input, funcname="count")
    result_df = common_tasks.postprocess_result.with_options(
        name="dist-alerts-by-natural-lands-postprocess-result"
    )(result_dataset)
    result_uri = common_tasks.save_result.with_options(
        name="dist-alerts-by-natural-lands-save-result"
    )(result_df, result_uri)

    return result_uri
