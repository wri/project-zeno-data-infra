from typing import Optional, Tuple

import pandas as pd
import xarray as xr
from prefect import task
from shapely.geometry import Polygon

from pipelines.land_ghg_inventory import (
    agriculture_stages,
    mineral_soil_stages,
    organic_soil_stages,
    vegetation_stages,
)
from pipelines.land_ghg_inventory.create_agriculture_zarr import (
    create_agriculture_zarr,
)


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
def prepare_agriculture_zarr(overwrite: bool = False) -> str:
    return create_agriculture_zarr(overwrite=overwrite)


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


@task
def load_mineral_soil(
    soc_uri: str,
    pixel_area_uri: str,
    country_uri: str,
    region_uri: str,
    subregion_uri: str,
    bbox: Optional[Polygon] = None,
) -> Tuple[xr.Dataset, xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray]:
    return mineral_soil_stages.load_data(
        soc_uri, pixel_area_uri, country_uri, region_uri, subregion_uri, bbox
    )


@task
def setup_mineral_soil_compute(datasets: Tuple, expected_groups: Tuple) -> Tuple:
    return mineral_soil_stages.setup_mineral_soil_compute(datasets, expected_groups)


@task
def mineral_soil_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    return mineral_soil_stages.mineral_soil_result_dataframe(reduced)


@task
def load_organic_soil(
    organic_soil_uri: str,
    pixel_area_uri: str,
    country_uri: str,
    region_uri: str,
    subregion_uri: str,
    bbox: Optional[Polygon] = None,
) -> Tuple[xr.Dataset, xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray]:
    return organic_soil_stages.load_data(
        organic_soil_uri, pixel_area_uri, country_uri, region_uri, subregion_uri, bbox
    )


@task
def setup_organic_soil_compute(datasets: Tuple, expected_groups: Tuple) -> Tuple:
    return organic_soil_stages.setup_organic_soil_compute(datasets, expected_groups)


@task
def organic_soil_result_dataframe(reduced: xr.DataArray) -> pd.DataFrame:
    return organic_soil_stages.organic_soil_result_dataframe(reduced)
