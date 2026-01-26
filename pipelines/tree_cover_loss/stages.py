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
    tree_cover_loss_uri: str,
    pixel_area_uri: Optional[str] = None,
    carbon_emissions_uri: Optional[str] = None,
    tree_cover_density_uri: Optional[str] = None,
    ifl_uri: Optional[str] = None,
    drivers_uri: Optional[str] = None,
    primary_forests_uri: Optional[str] = None,
) -> Tuple[xr.DataArray, xr.Dataset, xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray, xr.DataArray]:
    """
    Load in the tree cover loss zarr, pixel area zarr, carbon emissions zarr, tree cover density zarr, and the GADM zarrs
    Returns xr.DataArray for TCL and contextual layers and xr.Dataset for pixel area/carbon emissions
    """

    tcl: xr.DataArray = _load_zarr(tree_cover_loss_uri).band_data

    # load and align zarrs with tcl

    # aggregation layers
    pixel_area: xr.DataArray = _load_zarr(pixel_area_uri).band_data
    pixel_area = xr.align(tcl, pixel_area.reindex_like(tcl, method='nearest', tolerance=1e-5), join="left")[1]

    carbon_emissions: xr.DataArray = _load_zarr(carbon_emissions_uri).carbon_emissions_MgCO2e
    carbon_emissions = xr.align(tcl, carbon_emissions.reindex_like(tcl, method='nearest', tolerance=1e-5), join="left")[1]

    # contextual layers
    tcd: xr.DataArray = _load_zarr(tree_cover_density_uri).band_data
    tcd = xr.align(tcl, tcd.reindex_like(tcl, method='nearest', tolerance=1e-5), join="left")[1]

    ifl: xr.DataArray = _load_zarr(ifl_uri).band_data
    ifl = xr.align(tcl, ifl.reindex_like(tcl, method='nearest', tolerance=1e-5), join="left")[1].astype(np.int16)

    drivers: xr.DataArray = _load_zarr(drivers_uri).band_data
    drivers = xr.align(tcl, drivers.reindex_like(tcl, method='nearest', tolerance=1e-5), join="left")[1].astype(np.int16)

    primary_forests: xr.DataArray = _load_zarr(primary_forests_uri).band_data
    primary_forests = xr.align(tcl, primary_forests.reindex_like(tcl, method='nearest', tolerance=1e-5), join="left")[1]

    # GADM zarrs
    country: xr.DataArray = _load_zarr(country_zarr_uri).band_data
    country = xr.align(tcl, country.reindex_like(tcl, method='nearest', tolerance=1e-5), join="left")[1].astype(np.int16)

    region: xr.DataArray = _load_zarr(region_zarr_uri).band_data
    region = xr.align(tcl, region.reindex_like(tcl, method='nearest', tolerance=1e-5), join="left")[1].astype(np.uint8)

    subregion: xr.DataArray = _load_zarr(subregion_zarr_uri).band_data
    subregion = xr.align(tcl, subregion.reindex_like(tcl, method='nearest', tolerance=1e-5), join="left")[1].astype(np.int16)

    # combine area with emissions to sum both together
    area_and_emissions = xr.Dataset({
        "area_ha": pixel_area,
        "carbon__Mg_CO2e": carbon_emissions
    })

    return (
        tcl,
        area_and_emissions,
        tcd,
        ifl,
        drivers,
        primary_forests,
        country,
        region,
        subregion,
    )


def setup_compute(
    datasets: Tuple,
    expected_groups: Optional[ExpectedGroupsType],
) -> Tuple:
    """Setup the arguments for the xarray reduce on tree cover loss by area and emissions"""
    tcl, area_and_emissions, tcd, ifl, drivers, primary_forests, country, region, subregion = datasets

    # stack area and emissions into a single xarray
    mask = xr.concat(
        [area_and_emissions["area_ha"], area_and_emissions["carbon__Mg_CO2e"]],
        pd.Index(["area_ha", "carbon_Mg_CO2e"], name="layer")
    )

    groupbys: Tuple[xr.DataArray, ...] = (
        tcl.rename("tree_cover_loss_year"),
        tcd.rename("canopy_cover"),
        ifl.rename("is_intact_forest"),
        drivers.rename("driver"),
        primary_forests.rename("is_primary_forest"),
        country.rename("country"),
        region.rename("region"),
        subregion.rename("subregion"),
    )

    return (mask, groupbys, expected_groups)


def create_result_dataframe(result: xr.DataArray) -> pd.DataFrame:
    """
    Convert an xarray with multiple layers to a result df
    """
    # extract sparse data
    sparse_data = result.data
    dim_names = result.dims
    indices = sparse_data.coords
    values = sparse_data.data

    # create coordinate dictionary
    coord_dict = {
        dim: result.coords[dim].values[indices[i]]
        for i, dim in enumerate(dim_names)
    }
    coord_dict["value"] = values

    df = pd.DataFrame(coord_dict)

    # pivot to get separate cols for each layer
    df_pivoted = df.pivot_table(
        index=[col for col in df.columns if col not in ["layer", "value"]],
        columns="layer",
        values="value",
        fill_value=0
    ).reset_index()
    df_pivoted.columns.name = None

    return df_pivoted
