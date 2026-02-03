from typing import Callable, Optional, Tuple

import ee
import numpy as np
import pandas as pd
import xarray as xr
from shapely import box
from shapely.geometry import Polygon

from pipelines.globals import (
    country_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.prefect_flows import common_stages
from pipelines.prefect_flows.common_stages import _load_zarr
from pipelines.tree_cover_loss.prefect_flows.tcl import umd_tree_cover_loss

LoaderType = Callable[[str, Optional[str]], Tuple[xr.Dataset, ...]]
ExpectedGroupsType = Tuple
SaverType = Callable[[pd.DataFrame, str], None]


class TreeCoverLossTasks:
    @staticmethod
    def load_data(
        tree_cover_loss_uri: str,
        pixel_area_uri: Optional[str] = None,
        carbon_emissions_uri: Optional[str] = None,
        tree_cover_density_uri: Optional[str] = None,
        ifl_uri: Optional[str] = None,
        drivers_uri: Optional[str] = None,
        primary_forests_uri: Optional[str] = None,
        bbox: Optional[Polygon] = None,
    ) -> Tuple[
        xr.DataArray,
        xr.Dataset,
        xr.DataArray,
        xr.DataArray,
        xr.DataArray,
        xr.DataArray,
        xr.DataArray,
        xr.DataArray,
        xr.DataArray,
    ]:
        """
        Load in the tree cover loss zarr, pixel area zarr, carbon emissions zarr, tree cover density zarr, and the GADM zarrs
        Returns xr.DataArray for TCL and contextual layers and xr.Dataset for pixel area/carbon emissions
        """

        tcl: xr.DataArray = _load_zarr(tree_cover_loss_uri).band_data
        if bbox is not None:
            min_x, min_y, max_x, max_y = bbox.bounds
            # TODO assumption about zarr coords, wrap in class
            tcl = tcl.sel(x=slice(min_x, max_x), y=slice(max_y, min_y))

        # load and align zarrs with tcl

        # aggregation layers
        pixel_area: xr.DataArray = _load_zarr(pixel_area_uri).band_data
        pixel_area = xr.align(
            tcl,
            pixel_area.reindex_like(tcl, method="nearest", tolerance=1e-5),
            join="left",
        )[1]

        carbon_emissions: xr.DataArray = _load_zarr(
            carbon_emissions_uri
        ).carbon_emissions_MgCO2e
        carbon_emissions = xr.align(
            tcl,
            carbon_emissions.reindex_like(tcl, method="nearest", tolerance=1e-5),
            join="left",
        )[1]

        # contextual layers
        tcd: xr.DataArray = _load_zarr(tree_cover_density_uri).band_data
        tcd = xr.align(
            tcl, tcd.reindex_like(tcl, method="nearest", tolerance=1e-5), join="left"
        )[1]

        ifl: xr.DataArray = _load_zarr(ifl_uri).band_data
        ifl = xr.align(
            tcl, ifl.reindex_like(tcl, method="nearest", tolerance=1e-5), join="left"
        )[1].astype(np.int16)

        drivers: xr.DataArray = _load_zarr(drivers_uri).band_data
        drivers = xr.align(
            tcl,
            drivers.reindex_like(tcl, method="nearest", tolerance=1e-5),
            join="left",
        )[1].astype(np.int16)

        primary_forests: xr.DataArray = _load_zarr(primary_forests_uri).band_data
        primary_forests = xr.align(
            tcl,
            primary_forests.reindex_like(tcl, method="nearest", tolerance=1e-5),
            join="left",
        )[1]

        # GADM zarrs
        country: xr.DataArray = _load_zarr(country_zarr_uri).band_data
        country = xr.align(
            tcl,
            country.reindex_like(tcl, method="nearest", tolerance=1e-5),
            join="left",
        )[1].astype(np.int16)

        region: xr.DataArray = _load_zarr(region_zarr_uri).band_data
        region = xr.align(
            tcl, region.reindex_like(tcl, method="nearest", tolerance=1e-5), join="left"
        )[1].astype(np.uint8)

        subregion: xr.DataArray = _load_zarr(subregion_zarr_uri).band_data
        subregion = xr.align(
            tcl,
            subregion.reindex_like(tcl, method="nearest", tolerance=1e-5),
            join="left",
        )[1].astype(np.int16)

        # combine area with emissions to sum both together
        area_and_emissions = xr.Dataset(
            {"area_ha": pixel_area, "carbon__Mg_CO2e": carbon_emissions}
        )

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

    @staticmethod
    def compute_zonal_stat(*args, **kwargs) -> xr.DataArray:
        return common_stages.compute(*args, **kwargs)

    @staticmethod
    def setup_compute(
        datasets: Tuple,
        expected_groups: Optional[ExpectedGroupsType],
    ) -> Tuple:
        """Setup the arguments for the xarray reduce on tree cover loss by area and emissions"""
        (
            tcl,
            area_and_emissions,
            tcd,
            ifl,
            drivers,
            primary_forests,
            country,
            region,
            subregion,
        ) = datasets

        mask = xr.concat(
            [area_and_emissions["area_ha"], area_and_emissions["carbon__Mg_CO2e"]],
            pd.Index(["area_ha", "carbon_Mg_CO2e"], name="layer"),
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

    @staticmethod
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
            fill_value=0,
        ).reset_index()
        df_pivoted.columns.name = None

        return df_pivoted

    @staticmethod
    def qc_against_validation_source():
        sample_stats = TreeCoverLossTasks.get_sample_statistics("BRB")
        validation_stats = TreeCoverLossTasks.get_validation_statistics("BRB")
        diff = (
            validation_stats["area_ha"] - sample_stats["area_ha"]
        ) / validation_stats["area_ha"]

        if (diff.abs() > 0.01).any():
            return False

        return True

    @staticmethod
    def get_sample_statistics(
        iso: str,
        geometry_lookup: Optional[Callable[[str], Polygon]] = None,
    ) -> pd.DataFrame:
        if geometry_lookup is None:
            geometry_lookup = TreeCoverLossTasks.geometry_lookup

        results = umd_tree_cover_loss(TreeCoverLossTasks(), bbox=geometry_lookup(iso))
        return results

    @staticmethod
    def geometry_lookup(iso: str) -> Polygon:
        if iso != "BRB":
            raise NotImplementedError()

        return box(
            -59.856,  # min longitude (west)
            12.845,  # min latitude (south)
            -59.215,  # max longitude (east)
            13.535,  # max latitude (north)
        )

    @staticmethod
    def get_validation_statistics(
        iso: str,
        ee_module=ee,
        initialize: bool = True,
    ) -> pd.DataFrame:
        if initialize:
            ee_module.Initialize()

        gfc = ee_module.Image("UMD/hansen/global_forest_change_2024_v1_12")
        loss = gfc.select("loss").selfMask()
        tree_cover = gfc.select("treecover2000")

        threshold_mask = tree_cover.gt(30)
        loss_tcd30 = loss.updateMask(threshold_mask)

        drivers24 = ee_module.Image(
            "projects/landandcarbon/assets/wri_gdm_drivers_forest_loss_1km/v1_2_2001_2024"
        ).select("classification")

        loss_drivers24 = loss_tcd30.multiply(drivers24).selfMask()

        permag = loss_drivers24.eq(1).selfMask()
        hard = loss_drivers24.eq(2).selfMask()
        shifting = loss_drivers24.eq(3).selfMask()
        logging = loss_drivers24.eq(4).selfMask()
        wildfire = loss_drivers24.eq(5).selfMask()
        settlements = loss_drivers24.eq(6).selfMask()
        natural = loss_drivers24.eq(7).selfMask()

        bands = [
            "permag",
            "hard",
            "shifting",
            "logging",
            "wildfire",
            "settlements",
            "natural",
        ]
        drivers = (
            ee_module.Image(1)
            .addBands([permag, hard, shifting, logging, wildfire, settlements, natural])
            .rename(["blank"] + bands)
            .select(bands)
        )

        proj_info = gfc.projection().getInfo()
        crs = proj_info["crs"]
        transform = proj_info["transform"]

        def get_regional_stats(im, region):
            area_ha_img = im.multiply(ee_module.Image.pixelArea().divide(10000))
            stats = area_ha_img.reduceRegion(
                reducer=ee_module.Reducer.sum().unweighted(),
                geometry=region.geometry(),
                crs=crs,
                crsTransform=transform,
                bestEffort=False,
                maxPixels=1e12,
                tileScale=16,
            )
            return ee_module.Feature(None, stats).copyProperties(
                region, region.propertyNames()
            )

        gadm = ee_module.FeatureCollection(
            "projects/wri-datalab/GADM_410/gadm_410_ISO_name_1degree_gridded"
        )
        country = gadm.filter(ee_module.Filter.eq("GID_0", iso))

        fc = country.map(lambda f: get_regional_stats(drivers, ee_module.Feature(f)))

        # Bring it back to the client as JSON (Python dict)
        return fc.getInfo()

    @staticmethod
    def postprocess_result(result: xr.DataArray) -> pd.DataFrame:
        return TreeCoverLossTasks.create_result_dataframe(result)
