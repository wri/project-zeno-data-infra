from typing import Optional, Tuple

import pandas as pd
import xarray as xr
from prefect import task
from shapely.geometry import Polygon

from pipelines.land_ghg_inventory import agriculture_stages, vegetation_stages


@task
def load_vegetation(
    vegetation_uri: str,
    pixel_area_uri: str,
    country_uri: str,
    region_uri: str,
    subregion_uri: str,
    bbox: Optional[Polygon] = None,
) -> Tuple[xr.Dataset, xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray]:
    return vegetation_stages.load_data(
        vegetation_uri, pixel_area_uri, country_uri, region_uri, subregion_uri, bbox
    )


@task
def setup_vegetation_compute(datasets: Tuple, expected_groups: Tuple) -> Tuple:
    return vegetation_stages.setup_vegetation_compute(datasets, expected_groups)


@task
def vegetation_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    return vegetation_stages.vegetation_result_dataframe(reduced)


@task
def load_agriculture(
    agriculture_uri: str,
    country_uri: str,
    region_uri: str,
    subregion_uri: str,
    bbox: Optional[Polygon] = None,
) -> Tuple[xr.Dataset, xr.DataArray, xr.DataArray, xr.DataArray]:
    return agriculture_stages.load_agriculture(
        agriculture_uri, country_uri, region_uri, subregion_uri, bbox
    )


@task
def setup_agriculture_compute(datasets: Tuple, expected_groups: Tuple) -> Tuple:
    return agriculture_stages.setup_agriculture_compute(datasets, expected_groups)


@task
def agriculture_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    return agriculture_stages.agriculture_result_dataframe(reduced)
