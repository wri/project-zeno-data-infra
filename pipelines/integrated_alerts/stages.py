from typing import Callable, Optional, Tuple

import pandas as pd
import xarray as xr

from pipelines.prefect_flows.common_stages import (
    create_result_dataframe as common_create_result_dataframe,
)

LoaderType = Callable[[str, Optional[str]], Tuple[xr.Dataset, ...]]
ExpectedGroupsType = Tuple
SaverType = Callable[[pd.DataFrame, str], None]


def setup_compute(
    datasets: Tuple[xr.DataArray, ...],
    expected_groups: Optional[ExpectedGroupsType],
    contextual_column_name: Optional[str] = None,
) -> Tuple:
    """Setup the arguments for the xrarray reduce on natural lands by area"""
    base_zarr, country, region, subregion, contextual_layer = datasets

    mask = base_zarr.band_data
    groupbys: Tuple[xr.DataArray, ...] = (
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
    )
    if contextual_layer is not None:
        groupbys = groupbys + (contextual_layer.rename(contextual_column_name),)

    return (mask, groupbys, expected_groups)


def create_result_dataframe(alerts_count: xr.DataArray) -> pd.DataFrame:
    df = common_create_result_dataframe(alerts_count)
    del df["natural_lands"]
    return df
