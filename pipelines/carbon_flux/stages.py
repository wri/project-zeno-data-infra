from typing import Callable, Optional, Tuple
import pandas as pd
import xarray as xr

from pipelines.globals import (
    country_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)

from pipelines.prefect_flows.common_stages import create_result_dataframe as common_create_result_dataframe
LoaderType = Callable[[str, Optional[str]], Tuple[xr.Dataset, ...]]
ExpectedGroupsType = Tuple
SaverType = Callable[[pd.DataFrame, str], None]


def load_data(
        carbon_net_flux_zarr_uri,
        carbon_gross_removals_zarr_uri,
        carbon_gross_emissions_zarr_uri,
        tree_cover_density_2000_zarr_uri,
        tree_cover_loss_zarr_uri,
        mangrove_stock_2000_zarr_uri,
        tree_cover_gain_from_height_zarr_uri,
) -> Tuple[xr.DataArray, ...]:
    """Load in the layers needed for carbon flux analysis"""

    base_layer = _load_zarr(carbon_net_flux_zarr_uri)
    carbon_gross_removals = _load_zarr(carbon_gross_removals_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )
    carbon_gross_emissions = _load_zarr(carbon_gross_emissions_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )

    # reindex to dist alerts to avoid floating point precision issues
    # when aligning the datasets
    # https://github.com/pydata/xarray/issues/2217
    country = _load_zarr(country_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )
    country_aligned = xr.align(base_layer, country, join="left")[1].band_data
    region = _load_zarr(region_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )
    region_aligned = xr.align(base_layer, region, join="left")[1].band_data
    subregion = _load_zarr(subregion_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )
    subregion_aligned = xr.align(base_layer, subregion, join="left")[1].band_data

    mangrove_stock_2000 = _load_zarr(mangrove_stock_2000_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )
    mangrove_stock_2000_aligned = xr.align(base_layer, mangrove_stock_2000, join="left")[1].band_data

    tree_cover_gain_from_height = _load_zarr(tree_cover_gain_from_height_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )
    tree_cover_gain_from_height_aligned = xr.align(base_layer, tree_cover_gain_from_height, join="left")[1].band_data

    tree_cover_density_2000 = _load_zarr(tree_cover_density_2000_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )
    tree_cover_density_2000_aligned = xr.align(base_layer, tree_cover_density_2000, join="left")[1].band_data

    tree_cover_loss = _load_zarr(tree_cover_loss_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )
    tree_cover_loss_aligned = xr.align(base_layer, tree_cover_loss, join="left")[1].band_data

    return (
        base_layer,
        carbon_gross_removals,
        carbon_gross_emissions,
        country_aligned,
        region_aligned,
        subregion_aligned,
        mangrove_stock_2000_aligned,
        tree_cover_gain_from_height_aligned,
        tree_cover_density_2000_aligned,
        tree_cover_loss_aligned
    )


def setup_compute(
    datasets: Tuple[xr.DataArray, ...],
    expected_groups: Optional[ExpectedGroupsType],
    contextual_column_name: Optional[str] = None,
) -> Tuple:
    """Setup the arguments for the xrarray reduce on natural lands by area"""
    carbon_net_flux, carbon_gross_removals, carbon_gross_emissions, country, region, subregion, mangrove_stock_2000, tree_cover_gain_from_height, tree_cover_density_2000, tree_cover_loss = datasets

    # I created a data array with the multiple data inputs on a "carbontype"
    # dimension, rather than a dataset with multiple data variables, because only a
    # data array result works with the create_result_dataframe() post-processing of the
    # sparse data.
    ds = xr.concat([carbon_net_flux.band_data,
                    carbon_gross_removals.band_data,
                    carbon_gross_emissions.band_data],
                   pd.Index(["carbon_net_flux", "carbon_gross_removals", "carbon_gross_emissions"], name="carbontype"))

    groupbys: Tuple[xr.DataArray, ...] = (
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
        tree_cover_density_2000.rename("tree_cover_density"),
        tree_cover_loss.rename("tree_cover_loss"),
        mangrove_stock_2000.rename("mangrove_stock_2000"),
        tree_cover_gain_from_height.rename("tree_cover_gain_from_height"),
    )

    return (ds, groupbys, expected_groups)


def create_result_dataframe(alerts_count: xr.DataArray) -> pd.DataFrame:
    df = common_create_result_dataframe(alerts_count)

    return df


def _load_zarr(zarr_uri):
    return xr.open_zarr(zarr_uri)
