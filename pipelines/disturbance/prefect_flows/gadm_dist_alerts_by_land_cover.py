import numpy as np
import pandas as pd
from prefect import flow

from pipelines.disturbance.prefect_flows import dist_common_tasks
from pipelines.globals import land_cover_zarr_uri
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


@flow(name="DIST alerts area by land cover")
def dist_alerts_by_land_cover_area(
    dist_zarr_uri: str, dist_version: str, overwrite=False
):
    result_uri = f"{dist_common_tasks.DIST_PREFIX}/{dist_version}/admin-dist-alerts-by-land-cover-class.parquet"
    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(999),  # country ISO codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.arange(8),  # land cover classes
        np.arange(731, 2000),  # dates values
        [1, 2, 3],  # confidence values
    )
    datasets = dist_common_tasks.load_data.with_options(
        name="dist-alerts-by-land-cover-load-data"
    )(dist_zarr_uri, contextual_uri=land_cover_zarr_uri)
    # We only need year 2024 of the land cover contextual layer. We can fix later to
    # put this in a land-cover-specific setup_compute() task.
    datasets = datasets[:5] + (datasets[5].sel(year=2024),)
    compute_input = dist_common_tasks.setup_compute.with_options(
        name="set-up-dist-alerts-by-land-cover-compute"
    )(datasets, expected_groups, contextual_name="land_cover")

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="dist-alerts-by-land-cover-compute-zonal-stats"
    )(*compute_input, funcname="sum")
    result_df: pd.DataFrame = dist_common_tasks.postprocess_result.with_options(
        name="dist-alerts-by-land-cover-postprocess-result"
    )(result_dataset)

    result_df["land_cover"] = result_df["land_cover"].apply(
        lambda x: LAND_COVER_MAPPING.get(x, "Unclassified")
    )

    result_uri = common_tasks.save_result.with_options(
        name="dist-alerts-by-land-cover-save-result"
    )(result_df, result_uri)

    return result_uri
