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
        mangrove_stock_2000_zarr_uri,
        tree_cover_gain_from_height_zarr_uri,
) -> Tuple[xr.DataArray, ...]:
    """Load in the layers needed for carbon flux analysis"""

    base_layer = _load_zarr(carbon_net_flux_zarr_uri).astype("float64")
    carbon_gross_removals = _load_zarr(carbon_gross_removals_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    ).astype("float64")
    carbon_gross_emissions = _load_zarr(carbon_gross_emissions_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    ).astype("float64")

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

    # Very important "fill_value = 0" argument here, since mangroves has lots of
    # non-existent tiles, so the zarr file has a much smaller extent than the other
    # global datasets (including the base carbon layer). This effectively means those
    # areas will be filled with NoData/Nans, which will mess up the groupby. So, we
    # make sure to fill all those empty areas with 0 during the reindex to the base
    # layer.
    mangrove_stock_2000 = _load_zarr(mangrove_stock_2000_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5, fill_value=0
    )
    mangrove_stock_2000_aligned = xr.align(base_layer, mangrove_stock_2000, join="left")[1].band_data

    tree_cover_gain_from_height = _load_zarr(tree_cover_gain_from_height_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )
    tree_cover_gain_from_height_aligned = xr.align(base_layer, tree_cover_gain_from_height, join="left")[1].band_data
    # Map tree_cover_gain_from_height to 1 if any gain, 0 otherwise.
    tree_cover_gain_from_height_aligned = xr.where(tree_cover_gain_from_height_aligned > 0, 1, 0).astype("uint8")

    tree_cover_density_2000 = _load_zarr(tree_cover_density_2000_zarr_uri).reindex_like(
        base_layer, method="nearest", tolerance=1e-5
    )
    tree_cover_density_2000_aligned = xr.align(base_layer, tree_cover_density_2000, join="left")[1].band_data
    # Only keep the 30/50/75 densities (codes 5, 6, and 7), map the rest (10, 15, 20,
    # 25) to 0, which means less than 30% density.
    tree_cover_density_2000_aligned = tree_cover_density_2000_aligned.where(
        tree_cover_density_2000_aligned >= 5, other=0)

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
    )


def setup_compute(
    datasets: Tuple[xr.DataArray, ...],
    expected_groups: Optional[ExpectedGroupsType],
    contextual_column_name: Optional[str] = None,
) -> Tuple:
    """Setup the arguments for the xrarray reduce on natural lands by area"""
    carbon_net_flux, carbon_gross_removals, carbon_gross_emissions, country, region, subregion, mangrove_stock_2000, tree_cover_gain_from_height, tree_cover_density_2000 = datasets

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
        mangrove_stock_2000.rename("mangrove_stock_2000"),
        tree_cover_gain_from_height.rename("tree_cover_gain_from_height"),
    )

    return (ds, groupbys, expected_groups)


# tcd threshold mapping
thresh_to_pct = {
    0: 0,
    1: 10,
    2: 15,
    3: 20,
    4: 25,
    5: 30,
    6: 50,
    7: 75,
}


def create_result_dataframe(alerts_count: xr.DataArray) -> pd.DataFrame:
    df = common_create_result_dataframe(alerts_count)

    # Convert tcd thresholds to percentages
    df["tree_cover_density"] = df["tree_cover_density"].map(thresh_to_pct)

    # For each tcd value T in [30, 50, 75], aggregate carbon emissions/removal/flux if
    # (tree_cover_density >= T) OR (mangrove_stock_2000 is True) OR (tree_cover_gain_from_height is non-zero)
    thresholds = [30, 50, 75]
    results = []

    for T in thresholds:
        mask = ((df['tree_cover_density'] >= T)
                | (df['mangrove_stock_2000'] == 1)
                | (df['tree_cover_gain_from_height'] == 1)
                )

        temp_df = df[mask].groupby(
            ['country', 'region', 'subregion', 'carbontype'],
            as_index=False
        )['value'].sum()
        temp_df['tree_cover_density'] = T
        results.append(temp_df)

    df = pd.concat(results, ignore_index=True)
    df = df[['country', 'region', 'subregion', 'tree_cover_density', 'carbontype', 'value']]

    return df


def _load_zarr(zarr_uri):
    return xr.open_zarr(zarr_uri)
