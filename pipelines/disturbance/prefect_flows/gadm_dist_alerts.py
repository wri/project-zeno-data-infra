import numpy as np
from prefect import flow

from pipelines.disturbance.prefect_flows import dist_common_tasks
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists


@flow(name="DIST alerts area")
def dist_alerts_area(dist_zarr_uri: str, dist_version: str, overwrite=False):
    result_uri = (
        f"{dist_common_tasks.DIST_PREFIX}/{dist_version}/admin-dist-alerts.parquet"
    )
    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(999),  # country ISO codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.arange(731, 2000),  # dates values
        [1, 2, 3],  # confidence values
    )

    # load zarrs and align with pixel_area
    datasets = dist_common_tasks.load_data.with_options(name="dist-alerts-load_data")(
        dist_zarr_uri
    )
    # Datasets returned as: (dist_alerts, country, region, subregion, pixel_area)

    compute_input = dist_common_tasks.setup_compute.with_options(
        name="set-up-dist-alerts-compute"
    )(datasets, expected_groups)

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="dist-alerts-compute-zonal-stats"
    )(*compute_input, funcname="sum")
    result_df = dist_common_tasks.postprocess_result.with_options(
        name="dist-alerts-postprocess-result"
    )(result_dataset)
    common_tasks.save_result.with_options(name="dist-alerts-save-result")(
        result_df, result_uri
    )

    return result_uri
