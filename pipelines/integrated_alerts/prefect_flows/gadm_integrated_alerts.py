import numpy as np
from prefect import flow, task

from pipelines.integrated_alerts.prefect_flows import integrated_alerts_common_tasks
from pipelines.integrated_alerts.qc import qc_against_gee
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists


@task(name="integrated-alerts-qc")
def qc_integrated_alerts(result_df, version):
    return qc_against_gee(result_df, version)


@flow(name="Integrated alerts area", retries=2, retry_delay_seconds=120)
def integrated_alerts_area(
    integrated_alerts_zarr_uri: str, version: str, overwrite=False, run_qc=True
):
    result_uri = (
        f"{integrated_alerts_common_tasks.INTEGRATED_ALERTS_PREFIX}"
        f"/{version}/admin-integrated-alerts.parquet"
    )
    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(999),  # country ISO codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.arange(
            2923, 5000
        ),  # number of days since 2014/12/31 for (2023/1/1, 2028/9/8)
        [2, 3, 4],  # confidence values: 2=low, 3=high, 4=highest
    )

    # load zarrs and align with pixel_area
    datasets = integrated_alerts_common_tasks.load_data.with_options(
        name="integrated-alerts-load_data"
    )(integrated_alerts_zarr_uri)
    # Datasets returned as: (integrated_alerts, country, region, subregion, pixel_area)

    compute_input = integrated_alerts_common_tasks.setup_compute.with_options(
        name="set-up-integrated-alerts-compute"
    )(datasets, expected_groups)

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="integrated-alerts-compute-zonal-stats"
    )(*compute_input, funcname="sum")
    result_df = integrated_alerts_common_tasks.postprocess_result.with_options(
        name="integrated-alerts-postprocess-result"
    )(result_dataset)

    if run_qc and not qc_integrated_alerts(result_df, version):
        raise AssertionError(
            "Integrated alerts QC failed against the GEE tropics asset"
        )

    common_tasks.save_result.with_options(name="integrated-alerts-save-result")(
        result_df, result_uri
    )

    return result_uri
