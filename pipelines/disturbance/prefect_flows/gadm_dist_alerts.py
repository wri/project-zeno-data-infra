import numpy as np

from prefect import flow

from pipelines.disturbance.prefect_flows import dist_common_tasks


@flow(name="DIST alerts count")
def dist_alerts_count(dist_zarr_uri: str, dist_version: str):
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
    )(result_df, dist_version, "dist_alerts")

    return result_uri
