from typing import Callable, Optional, Tuple
import pandas as pd
import xarray as xr

LoaderType = Callable[[str, Optional[str]], Tuple[xr.Dataset, ...]]
ExpectedGroupsType = Tuple
SaverType = Callable[[pd.DataFrame, str], None]

def setup_compute(
    datasets: Tuple[xr.Dataset, ...],
    expected_groups: Optional[ExpectedGroupsType],
    contextual_column_name: Optional[str] = None,
) -> Tuple:
    """Setup the arguments for the xrarray reduce on natural lands by area"""
    base_zarr, country, region, subregion, contextual_layer = datasets

    mask = base_zarr.band_data
    groupbys: Tuple[xr.Dataset, ...] = (
        country.band_data.rename("country"),
        region.band_data.rename("region"),
        subregion.band_data.rename("subregion"),
    )
    if contextual_layer is not None:
        groupbys = (
            groupbys + (contextual_layer.band_data.rename(contextual_column_name),)
        )

    return (mask, groupbys, expected_groups)
