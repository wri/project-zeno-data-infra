from typing import Callable, Optional, Tuple
import pandas as pd
import xarray as xr
import numpy as np

from pipelines.globals import (
    country_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.prefect_flows.common_stages import _load_zarr

LoaderType = Callable[[str, Optional[str]], Tuple[xr.Dataset, ...]]
ExpectedGroupsType = Tuple
SaverType = Callable[[pd.DataFrame, str], None]

def load_data(
    pixel_area_uri: str,
    grasslands_uri: Optional[str] = None,
) -> Tuple[xr.DataArray, ...]:
    """Load in the area zarr, grasslands zarr, and the GADM zarrs"""

    grasslands: xr.DataArray = _load_zarr(grasslands_uri).band_data
    xmin, xmax, ymin, ymax = (
        grasslands.coords['x'].min().item(), grasslands.coords['x'].max().item() + 0.00025,
        grasslands.coords['y'].min().item() - 0.00025, grasslands.coords['y'].max().item()
    )

    pixel_area: xr.DataArray = _load_zarr(pixel_area_uri).band_data.sel(x=slice(xmin, xmax), y=slice(ymax, ymin)).chunk({'x': 2000, 'y': 2000})

    country: xr.DataArray = _load_zarr(country_zarr_uri).band_data.sel(x=slice(xmin, xmax), y=slice(ymax, ymin)).chunk({'x': 2000, 'y': 2000})
    region: xr.DataArray = _load_zarr(region_zarr_uri).band_data.sel(x=slice(xmin, xmax), y=slice(ymax, ymin)).chunk({'x': 2000, 'y': 2000})
    subregion: xr.DataArray = _load_zarr(subregion_zarr_uri).band_data.sel(x=slice(xmin, xmax), y=slice(ymax, ymin)).chunk({'x': 2000, 'y': 2000})

    grasslands.coords['x'] = pixel_area.coords['x']
    grasslands.coords['y'] = pixel_area.coords['y']

    # set all coords pixel area coords
    country.coords['x'] = pixel_area.coords['x']
    country.coords['y'] = pixel_area.coords['y']
    region.coords['x'] = pixel_area.coords['x']
    region.coords['y'] = pixel_area.coords['y']
    subregion.coords['x'] = pixel_area.coords['x']
    subregion.coords['y'] = pixel_area.coords['y']

    pixel_area = pixel_area.drop_vars('band').squeeze()
    grasslands_only = (grasslands == 2).astype(np.uint8)

    grasslands_areas = grasslands_only * pixel_area

    return (
        grasslands_areas,
        country,
        region,
        subregion,
    )


def setup_compute(
    datasets: Tuple[xr.DataArray, ...],
    expected_groups: Optional[ExpectedGroupsType],
    contextual_column_name: Optional[str] = None,
) -> Tuple:
    """Setup the arguments for the xrarray reduce on grasslands by area"""
    base_zarr, country, region, subregion = datasets

    mask = base_zarr
    groupbys: Tuple[xr.DataArray, ...] = (
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
    )

    return (mask, groupbys, expected_groups)
