import logging

import numpy as np
from prefect import flow

from pipelines.carbon_flux.prefect_flows import carbon_tasks
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists
from pipelines.globals import (
    tree_cover_density_2000_zarr_uri,
    mangrove_stock_2000_zarr_uri,
    tree_cover_gain_from_height_zarr_uri
)


@flow(name="Carbon flux")
def gadm_carbon_flux(overwrite: bool = False):
    logging.getLogger("distributed.client").setLevel(logging.ERROR)  # or logging.ERROR

    carbon_zarr_uris = carbon_tasks.create_zarrs.with_options(
        name="create-carbon-zarrs"
    )(overwrite=overwrite)

    result_uri = "s3://lcl-analytics/zonal-statistics/admin-carbon.parquet"

    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(999),  # country iso codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.arange(8),  # tree cover density
        [0, 1],  # mangrove boolean
        [0, 1],  # tree cover gain from height boolean
    )

    datasets = carbon_tasks.load_data.with_options(name="carbon-flux-load-data")(
        carbon_zarr_uris["carbon_net_flux"],
        carbon_zarr_uris["carbon_gross_removals"],
        carbon_zarr_uris["carbon_gross_emissions"],
        tree_cover_density_2000_zarr_uri,
        mangrove_stock_2000_zarr_uri,
        tree_cover_gain_from_height_zarr_uri,
        group="pipeline",
    )

    compute_input = carbon_tasks.setup_compute.with_options(
        name="carbon-flux-setup-compute"
    )(datasets, expected_groups)

    result_dataset = common_tasks.compute_zonal_stat.with_options(
        name="carbon-flux-compute-zonal-stats"
    )(*compute_input, funcname="sum")
    result_df = carbon_tasks.postprocess_result.with_options(
        name="carbon-flux-postprocess-result"
    )(result_dataset)
    result_uri = common_tasks.save_result.with_options(name="carbon-flux-save-result")(
        result_df, result_uri
    )
    return result_uri
