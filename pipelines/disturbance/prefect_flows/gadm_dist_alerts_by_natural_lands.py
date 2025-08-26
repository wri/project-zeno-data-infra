import numpy as np
import pandas as pd

from prefect import flow

from pipelines.disturbance.prefect_flows import dist_common_tasks
from pipelines.globals import DATA_LAKE_BUCKET
from pipelines.utils import s3_uri_exists
from pipelines.prefect_flows import common_tasks

NATURAL_LANDS_CLASSES = {
    2: "Natural forests",
    3: "Natural short vegetation",
    4: "Natural water",
    5: "Mangroves",
    6: "Bare",
    7: "Snow",
    8: "Wetland natural forests",
    9: "Natural peat forests",
    10: "Wetland natural short vegetation",
    11: "Natural peat short vegetation",
    12: "Cropland",
    13: "Built-up",
    14: "Non-natural tree cover",
    15: "Non-natural short vegetation",
    16: "Non-natural water",
    17: "Wetland non-natural tree cover",
    18: "Non-natural peat tree cover",
    19: "Wetland non-natural short vegetation",
    20: "Non-natural peat short vegetation",
    21: "Non-natural bare",
}


@flow(name="DIST alerts area by natural lands")
def dist_alerts_by_natural_lands_area(dist_zarr_uri: str, dist_version: str, overwrite=False):
    result_filename = "dist_alerts_by_natural_lands"
    result_uri = f"s3://{DATA_LAKE_BUCKET}/umd_glad_dist_alerts/{dist_version}/tabular/zonal_stats/gadm/gadm_adm2_{result_filename}.parquet"
    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(999),  # country ISO codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.arange(22),  # natural lands categories
        np.arange(731, 2000),  # dates values
        [1, 2, 3],  # confidence values
    )
    contextual_uri = f"s3://{DATA_LAKE_BUCKET}/sbtn_natural_lands/zarr/sbtn_natural_lands_all_classes.zarr"
    datasets = dist_common_tasks.load_data.with_options(
        name="dist-alerts-by-natural-lands-load-data"
    )(dist_zarr_uri, contextual_uri=contextual_uri)
    compute_input = dist_common_tasks.setup_compute.with_options(
        name="set-up-dist-alerts-by-natural-lands-compute"
    )(datasets, expected_groups, contextual_name="natural_land_class")

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="dist-alerts-by-natural-lands-compute-zonal-stats"
    )(*compute_input, funcname="sum")
    result_df: pd.DataFrame = dist_common_tasks.postprocess_result.with_options(
        name="dist-alerts-by-natural-lands-postprocess-result"
    )(result_dataset)

    # natural_land_class
    result_df["natural_land_class"] = result_df["natural_land_class"].apply(
        lambda x: NATURAL_LANDS_CLASSES.get(x, "Unclassified")
    )

    result_uri = common_tasks.save_result.with_options(
        name="dist-alerts-by-natural-lands-save-result"
    )(result_df, result_uri)

    return result_uri
