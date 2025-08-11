import numpy as np

from prefect import flow

from pipelines.disturbance.prefect_flows import dist_common_tasks
from pipelines.globals import DATA_LAKE_BUCKET
from pipelines.utils import s3_uri_exists
from pipelines.prefect_flows import common_tasks

@flow(name="DIST alerts area")
def dist_alerts_count(dist_zarr_uri: str, dist_version: str, overwrite=False):

    area_uri = "s3://gfw-data-lake/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area.zarr"
    result_filename = "dist_alerts"
    result_uri = f"s3://{DATA_LAKE_BUCKET}/umd_glad_dist_alerts/{dist_version}/tabular/zonal_stats/gadm/gadm_adm2_{result_filename}.parquet"
    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(894),  # country ISO codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.arange(731, 2000),  # dates values
        [1, 2, 3],  # confidence values
    )

    # load zarrs and align with pixel_area
    datasets = common_tasks.load_data.with_options(name="dist-alerts-load_data")(
        area_uri, contextual_uri=dist_zarr_uri
    )
    # Datasets returned as: (pixel_area, country, region, subregion, dist_alerts)
    # TODO refactor stages.load_data to not hardcode distuSE alerts as the base layer

    compute_input = dist_common_tasks.setup_compute.with_options(
        name="set-up-dist-alerts-compute"
    )(datasets, expected_groups)

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="dist-alerts-compute-zonal-stats"
    )(*compute_input, funcname="sum")
    result_df = dist_common_tasks.postprocess_result.with_options(
        name="dist-alerts-postprocess-result"
    )(result_dataset)
    common_tasks.save_result.with_options(
        name="dist-alerts-save-result"
    )(result_df, result_uri)

    return result_uri
