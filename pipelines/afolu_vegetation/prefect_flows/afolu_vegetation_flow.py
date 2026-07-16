import logging
from typing import Optional

import numpy as np
from prefect import flow, task

from pipelines.afolu_vegetation import qc, reference
from pipelines.afolu_vegetation.prefect_flows import afolu_vegetation_tasks
from pipelines.globals import (
    ANALYTICS_BUCKET,
    afolu_vegetation_zarr_uri,
    country_zarr_uri,
    pixel_area_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists


@task(name="afolu-vegetation-qc")
def qc_afolu_vegetation(result_df, reference_uri) -> bool:
    reference_totals = reference.load_reference_totals(reference_uri)
    return qc.qc_against_reference(result_df, reference_totals)


@flow(name="AFOLU vegetation area", retries=2, retry_delay_seconds=120)
def afolu_vegetation_area(
    version: str,
    overwrite: bool = False,
    reference_uri: Optional[str] = None,
):
    logging.getLogger("distributed.client").setLevel(logging.ERROR)
    result_uri = (
        f"s3://{ANALYTICS_BUCKET}/zonal-statistics/afolu-vegetation/"
        f"{version}/admin-afolu-vegetation.parquet"
    )
    if not overwrite and s3_uri_exists(result_uri):
        return result_uri

    expected_groups = (
        np.arange(999),  # country iso codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.array([0, 1, 2, 3, 4]),  # vegetation category codes
        np.arange(9),  # year index 0..8 -> 2016..2024
    )

    datasets = afolu_vegetation_tasks.load_data.with_options(
        name="afolu-vegetation-load-data"
    )(
        afolu_vegetation_zarr_uri,
        pixel_area_zarr_uri,
        country_zarr_uri,
        region_zarr_uri,
        subregion_zarr_uri,
    )

    compute_input = afolu_vegetation_tasks.setup_compute.with_options(
        name="set-up-afolu-vegetation-compute"
    )(datasets, expected_groups)

    reduced = common_tasks.compute_zonal_stat.with_options(
        name="afolu-vegetation-compute-zonal-stats"
    )(*compute_input, funcname="sum")

    result_df = afolu_vegetation_tasks.postprocess_result.with_options(
        name="afolu-vegetation-postprocess-result"
    )(reduced)

    if reference_uri is not None and not qc_afolu_vegetation(result_df, reference_uri):
        raise AssertionError("AFOLU vegetation QC failed against the reference dataset")

    return common_tasks.save_result.with_options(name="afolu-vegetation-save-result")(
        result_df, result_uri
    )
