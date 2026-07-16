from typing import Optional, Tuple

import pandas as pd
import xarray as xr
from prefect import task
from shapely.geometry import Polygon

from pipelines.afolu import stages


@task
def load_vegetation(
    vegetation_uri: str,
    pixel_area_uri: str,
    country_uri: str,
    region_uri: str,
    subregion_uri: str,
    bbox: Optional[Polygon] = None,
) -> Tuple[xr.Dataset, xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray]:
    return stages.load_data(
        vegetation_uri, pixel_area_uri, country_uri, region_uri, subregion_uri, bbox
    )


@task
def setup_vegetation_compute(datasets: Tuple, expected_groups: Tuple) -> Tuple:
    return stages.setup_vegetation_compute(datasets, expected_groups)


@task
def vegetation_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    return stages.vegetation_result_dataframe(reduced)


@task
def load_soil_mineral(
    soc_uri: str,
    pixel_area_uri: str,
    country_uri: str,
    region_uri: str,
    subregion_uri: str,
    bbox: Optional[Polygon] = None,
) -> Tuple[xr.Dataset, xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray]:
    return stages.load_soil_mineral(
        soc_uri, pixel_area_uri, country_uri, region_uri, subregion_uri, bbox
    )


@task
def setup_soil_mineral_compute(datasets: Tuple, expected_groups: Tuple) -> Tuple:
    return stages.setup_soil_mineral_compute(datasets, expected_groups)


@task
def soil_mineral_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    return stages.soil_mineral_result_dataframe(reduced)


@task
def combine_components(frames) -> pd.DataFrame:
    return pd.concat(frames, ignore_index=True)
