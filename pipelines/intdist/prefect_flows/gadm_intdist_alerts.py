import numpy as np
from prefect import flow

from pipelines.intdist.prefect_flows import intdist_common_tasks
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists


@flow(name="INTDIST alerts area", retries=2, retry_delay_seconds=120)
def intdist_alerts_area(intdist_zarr_uri: str, version: str, overwrite=False):
    result_uri = (
        f"{intdist_common_tasks.DIST_PREFIX}/{version}/admin-intdist-alerts.parquet"
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
    datasets = intdist_common_tasks.load_data.with_options(name="intdist-alerts-load_data")(
        intdist_zarr_uri
    )
    # Datasets returned as: (intdist_alerts, country, region, subregion, pixel_area)

    compute_input = intdist_common_tasks.setup_compute.with_options(
        name="set-up-intdist-alerts-compute"
    )(datasets, expected_groups)

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="intdist-alerts-compute-zonal-stats"
    )(*compute_input, funcname="sum")
    result_df = intdist_common_tasks.postprocess_result.with_options(
        name="intdist-alerts-postprocess-result"
    )(result_dataset)
    common_tasks.save_result.with_options(name="intdist-alerts-save-result")(
        result_df, result_uri
    )

    return result_uri
