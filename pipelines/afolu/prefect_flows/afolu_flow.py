import logging
from typing import Optional

import numpy as np
from prefect import flow, task
from shapely.geometry import Polygon

from pipelines.afolu import qc, reference
from pipelines.afolu.prefect_flows import afolu_tasks
from pipelines.globals import (
    ANALYTICS_BUCKET,
    afolu_soc_zarr_uri,
    afolu_vegetation_zarr_uri,
    country_zarr_uri,
    pixel_area_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.prefect_flows import common_tasks
from pipelines.utils import s3_uri_exists


@task(name="afolu-qc")
def qc_afolu(result_df, reference_uri) -> bool:
    reference_totals = reference.load_reference_totals(reference_uri)
    return qc.qc_against_reference(result_df, reference_totals)


def _vegetation_result_df(bbox=None):
    """Run the vegetation reduce and return its tidy component rows."""
    expected_groups = (
        np.arange(999),  # country iso codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.array([0, 1, 2, 3, 4]),  # vegetation category codes
        np.arange(9),  # year index 0..8 -> 2016..2024
    )
    datasets = afolu_tasks.load_vegetation.with_options(
        name="afolu-vegetation-load-data"
    )(
        afolu_vegetation_zarr_uri,
        pixel_area_zarr_uri,
        country_zarr_uri,
        region_zarr_uri,
        subregion_zarr_uri,
        bbox,
    )
    compute_input = afolu_tasks.setup_vegetation_compute.with_options(
        name="set-up-afolu-vegetation-compute"
    )(datasets, expected_groups)
    reduced = common_tasks.compute_zonal_stat.with_options(
        name="afolu-vegetation-compute-zonal-stats"
    )(*compute_input, funcname="sum")
    return afolu_tasks.vegetation_result_dataframe.with_options(
        name="afolu-vegetation-postprocess-result"
    )(reduced)


def _soil_mineral_result_df(bbox=None):
    """Run the mineral-soil reduce and return its tidy component rows."""
    expected_groups = (
        np.arange(999),  # country iso codes
        np.arange(86),  # region codes
        np.arange(854),  # subregion codes
        np.array([1]),  # soil category code (mineral)
        np.arange(5),  # SOC interval index 0..4
    )
    datasets = afolu_tasks.load_soil_mineral.with_options(
        name="afolu-mineral-soil-load-data"
    )(
        afolu_soc_zarr_uri,
        pixel_area_zarr_uri,
        country_zarr_uri,
        region_zarr_uri,
        subregion_zarr_uri,
        bbox,
    )
    compute_input = afolu_tasks.setup_soil_mineral_compute.with_options(
        name="set-up-afolu-mineral-soil-compute"
    )(datasets, expected_groups)
    reduced = common_tasks.compute_zonal_stat.with_options(
        name="afolu-mineral-soil-compute-zonal-stats"
    )(*compute_input, funcname="sum")
    return afolu_tasks.soil_mineral_result_dataframe.with_options(
        name="afolu-mineral-soil-postprocess-result"
    )(reduced)


@flow(name="AFOLU area", retries=2, retry_delay_seconds=120)
def afolu_area(
    version: str,
    overwrite: bool = False,
    reference_uri: Optional[str] = None,
    bbox: Optional[Polygon] = None,
):
    """AFOLU land-flux zonal stats. Each carbon pool (vegetation, mineral soil) is
    reduced independently and concatenated into one aoi_id-keyed parquet with the
    unified carbon_pool/flux_class schema. Organic soil is a follow-up pool.

    ``bbox`` clips the reduce to one area for a laptop-friendly local run; the
    result is written to a local ``admin-afolu-{version}.parquet`` instead of the
    canonical global S3 path (which would be a partial write)."""
    logging.getLogger("distributed.client").setLevel(logging.ERROR)

    if bbox is None:
        result_uri = (
            f"s3://{ANALYTICS_BUCKET}/zonal-statistics/afolu/"
            f"{version}/admin-afolu.parquet"
        )
        if not overwrite and s3_uri_exists(result_uri):
            return result_uri
    else:
        result_uri = f"admin-afolu-{version}.parquet"  # local bbox test run

    result_df = afolu_tasks.combine_components(
        [_vegetation_result_df(bbox), _soil_mineral_result_df(bbox)]
    )

    if reference_uri is not None and not qc_afolu(result_df, reference_uri):
        raise AssertionError("AFOLU QC failed against the reference dataset")

    return common_tasks.save_result.with_options(name="afolu-save-result")(
        result_df, result_uri
    )
