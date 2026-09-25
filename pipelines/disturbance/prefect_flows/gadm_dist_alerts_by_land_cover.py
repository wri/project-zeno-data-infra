import numpy as np
import pandas as pd
from prefect import flow

from pipelines.disturbance.prefect_flows import dist_common_tasks
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists

LAND_COVER_MAPPING = {
    0: "Bare and sparse vegetation",
    1: "Short vegetation",
    2: "Tree cover",
    3: "Wetland – short vegetation",
    4: "Water",
    5: "Snow/ice",
    6: "Cropland",
    7: "Built-up",
    8: "Cultivated grasslands",
}


@flow(name="DIST alerts area by land cover", retries=2, retry_delay_seconds=120)
def dist_alerts_by_land_cover_area(
    dist_zarr_uri: str, dist_version: str, overwrite=False
):
    result_uri = (
        f"{dist_common_tasks.DIST_PREFIX}/{dist_version}"
        "/admin-dist-alerts-by-land-cover-class.parquet"
    )
    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(999),  # country ISO codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.arange(9),  # land cover classes
        np.arange(731, 3288),  # dates values, 2023/1/1 to 2030/1/1
        [1, 2, 3],  # confidence values
    )
    datasets = dist_common_tasks.load_alert_pixels.with_options(
        name="dist-alerts-by-land-cover-load-data"
    )(dist_zarr_uri, contextual_name="land_cover")
    compute_input = dist_common_tasks.setup_compute.with_options(
        name="set-up-dist-alerts-by-land-cover-compute"
    )(datasets, expected_groups, contextual_name="land_cover")

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="dist-alerts-by-land-cover-compute-zonal-stats"
    )(*compute_input, funcname="sum")
    result_df: pd.DataFrame = dist_common_tasks.postprocess_result.with_options(
        name="dist-alerts-by-land-cover-postprocess-result"
    )(result_dataset)

    result_df["land_cover"] = (
        result_df["land_cover"].map(LAND_COVER_MAPPING).fillna("Unclassified")
    )

    result_uri = common_tasks.save_result.with_options(
        name="dist-alerts-by-land-cover-save-result"
    )(result_df, result_uri)

    return result_uri
