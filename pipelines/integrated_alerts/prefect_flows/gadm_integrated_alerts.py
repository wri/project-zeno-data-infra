import numpy as np
from prefect import flow

from pipelines.globals import DATA_LAKE_BUCKET
from pipelines.integrated_alerts.prefect_flows import integrated_common_tasks
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists


@flow(name="DIST alerts area")
def integrated_alerts_area(
    integrated_zarr_uri: str, integrated_version: str, overwrite=False
):
    result_filename = "integrated_alerts"
    result_uri = f"s3://{DATA_LAKE_BUCKET}/umd_glad_integrated_alerts/{integrated_version}/tabular/zonal_stats/gadm/gadm_adm2_{result_filename}.parquet"
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
    datasets = integrated_common_tasks.load_data.with_options(
        name="integrated-alerts-load_data"
    )(integrated_zarr_uri)
    # Datasets returned as: (integrated_alerts, country, region, subregion, pixel_area)

    compute_input = integrated_common_tasks.setup_compute.with_options(
        name="set-up-integrated-alerts-compute"
    )(datasets, expected_groups)

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="integrated-alerts-compute-zonal-stats"
    )(*compute_input, funcname="sum")
    result_df = integrated_common_tasks.postprocess_result.with_options(
        name="integrated-alerts-postprocess-result"
    )(result_dataset)
    common_tasks.save_result.with_options(name="integrated-alerts-save-result")(
        result_df, result_uri
    )

    return result_uri
