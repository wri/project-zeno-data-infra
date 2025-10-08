from typing import Tuple

import numpy as np
import xarray as xr
from prefect import flow

from pipelines.disturbance.prefect_flows import dist_common_tasks
from pipelines.globals import DATA_LAKE_BUCKET
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists


@flow(name="DIST alerts area")
def dist_alerts_area(dist_zarr_uri: str, dist_version: str, overwrite=False) -> str:
    result_filename = "dist_alerts"
    result_uri = f"s3://{DATA_LAKE_BUCKET}/umd_glad_dist_alerts/{dist_version}/tabular/zonal_stats/gadm/gadm_adm2_{result_filename}.parquet"
    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    # fmt: off
    expected_groups = (
        np.arange(999),        # country ISO codes
        np.arange(86),         # region codes
        np.arange(854),        # subregion codes
        np.arange(731, 2000),  # dates values
        [1, 2, 3],             # confidence values
    )
    # fmt: on

    # load zarrs and align with pixel_area
    datasets: Tuple[
        xr.Dataset, xr.Dataset, xr.Dataset, xr.Dataset, xr.Dataset, xr.Dataset | None
    ] = dist_common_tasks.load_data.with_options(name="dist-alerts-load_data")(
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
