from typing import Optional, Tuple

import xarray as xr
from prefect import task
from shapely.geometry import Polygon

from pipelines.globals import ANALYTICS_BUCKET
from pipelines.integrated_alerts import alert_points_table, stages

INTEGRATED_ALERTS_PREFIX = f"s3://{ANALYTICS_BUCKET}/zonal-statistics/integrated-alerts"


@task
def load_data(
    zarr_uri: str, natural_lands_uri: str, bbox: Optional[Polygon] = None
) -> Tuple[xr.DataArray, ...]:
    return stages.load_data(zarr_uri, natural_lands_uri, bbox)


@task
def setup_compute(datasets: Tuple[xr.DataArray, ...], expected_groups) -> Tuple:
    return stages.setup_compute(datasets, expected_groups)


@task(persist_result=False)
def postprocess_result(result: xr.DataArray):
    return stages.create_result_dataframe(result)


@task(persist_result=False)
def build_alert_points_table(
    zarr_uri: str,
    admin_result_uri: str,
    version: str,
    result_uri: str,
    days_stored: int,
    bbox: Optional[Polygon] = None,
) -> dict:
    return alert_points_table.build_alert_points_table(
        zarr_uri, admin_result_uri, version, result_uri, days_stored, bbox
    )
