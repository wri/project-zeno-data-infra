from typing import Callable, Optional, Tuple
import pandas as pd
import xarray as xr

from pipelines.prefect_flows.common_stages import create_result_dataframe as common_create_result_dataframe
LoaderType = Callable[[str, Optional[str]], Tuple[xr.Dataset, ...]]
ExpectedGroupsType = Tuple
SaverType = Callable[[pd.DataFrame, str], None]

sbtn_natural_lands_classes = {
    2: "Natural forests",
    3: "Natural short vegetation",
    4: "Natural water",
    5: "Mangroves",
    6: "Bare",
    7: "Snow",
    8: "Wetland natural forests",
    9: "Natural peat forests",
    10: "Wetland natural short vegetation",
    11: "Natural peat short vegetation",
    12: "Cropland",
    13: "Built-up",
    14: "Non-natural tree cover",
    15: "Non-natural hhort vegetation",
    16: "Non-natural water",
    17: "Wetland non-natural tree cover",
    18: "Non-natural peat tree cover",
    19: "Wetland non-natural short vegetation",
    20: "Non-natural peat short vegetation",
    21: "Non-natural bare"
}


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


def create_result_dataframe(alerts_count: xr.Dataset) -> pd.DataFrame:
    df = common_create_result_dataframe(alerts_count)
    df["natural_lands_category"] = df.natural_lands.apply(lambda x: "natural" if 1 < x < 12 else "non-natural")
    df["natural_lands_class"] = df.natural_lands.apply(lambda x: sbtn_natural_lands_classes.get(x, "unclassified"))
    del df["natural_lands"]
    return df
