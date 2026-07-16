from typing import Optional, Tuple

import pandas as pd
import xarray as xr
from prefect import task
from shapely.geometry import Polygon

from pipelines.afolu_vegetation import stages


@task
def load_data(
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
def setup_compute(datasets: Tuple, expected_groups: Tuple) -> Tuple:
    return stages.setup_compute(datasets, expected_groups)


@task
def postprocess_result(reduced: xr.DataArray) -> pd.DataFrame:
    return stages.create_result_dataframe(reduced)
