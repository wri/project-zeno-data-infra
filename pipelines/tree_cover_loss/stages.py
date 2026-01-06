from typing import Callable, Optional, Tuple
import pandas as pd
import xarray as xr
import numpy as np

from pipelines.globals import (
    country_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
    pixel_area_zarr_uri,
)
from pipelines.prefect_flows.common_stages import _load_zarr

LoaderType = Callable[[str, Optional[str]], Tuple[xr.Dataset, ...]]
ExpectedGroupsType = Tuple
SaverType = Callable[[pd.DataFrame, str], None]

def load_data(
    tree_cover_loss_uri: str,
    pixel_area_uri: Optional[str] = None,
) -> Tuple[xr.DataArray, ...]:
    """Load in the tree cover loss zarr, pixel area zarr, and the GADM zarrs"""

    tcl: xr.DataArray = _load_zarr(tree_cover_loss_uri).band_data

    # load and align contextual zaars
    pixel_area: xr.DataArray = _load_zarr(pixel_area_uri).band_data
    pixel_area = xr.align(tcl, pixel_area.reindex_like(tcl, method='nearest', tolerance=1e-5), join="left")[1]

    country: xr.DataArray = _load_zarr(country_zarr_uri).band_data
    country = xr.align(tcl, country.reindex_like(tcl, method='nearest', tolerance=1e-5), join="left")[1].astype(np.int16)

    region: xr.DataArray = _load_zarr(region_zarr_uri).band_data
    region = xr.align(tcl, region.reindex_like(tcl, method='nearest', tolerance=1e-5), join="left")[1].astype(np.uint8)

    subregion: xr.DataArray = _load_zarr(subregion_zarr_uri).band_data
    subregion = xr.align(tcl, subregion.reindex_like(tcl, method='nearest', tolerance=1e-5), join="left")[1].astype(np.int16)
    
    # calculate loss area
    tcl_binary = (tcl > 0).astype(np.uint8)
    tcl_area = tcl_binary * pixel_area

    return (
        tcl,
        tcl_area,
        country,
        region,
        subregion,
    )


def setup_compute(
    datasets: Tuple[xr.DataArray, ...],
    expected_groups: Optional[ExpectedGroupsType],
    contextual_column_name: Optional[str] = None,
) -> Tuple:
    """Setup the arguments for the xarray reduce on tree cover loss by area"""
    tcl, tcl_area, country, region, subregion = datasets

    mask = tcl_area
    groupbys: Tuple[xr.DataArray, ...] = (
        tcl.rename(contextual_column_name if contextual_column_name else "tree_cover_loss_year"),
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
    )

    return (mask, groupbys, expected_groups)
