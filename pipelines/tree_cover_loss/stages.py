from typing import Callable, Dict, Optional, Tuple

import ee
import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import Polygon, mapping

from pipelines.globals import (
    country_zarr_uri,
    region_zarr_uri,
    subregion_zarr_uri,
)
from pipelines.prefect_flows import common_stages
from pipelines.prefect_flows.common_stages import _load_zarr
from pipelines.repositories.google_earth_engine_dataset_repository import (
    GoogleEarthEngineDatasetRepository,
)
from pipelines.repositories.qc_feature_repository import QCFeaturesRepository
from pipelines.tree_cover_loss.prefect_flows.tcl import compute_tree_cover_loss

LoaderType = Callable[[str, Optional[str]], Tuple[xr.Dataset, ...]]
ExpectedGroupsType = Tuple
SaverType = Callable[[pd.DataFrame, str], None]

from pipelines.prefect_flows.common_stages import numeric_to_alpha3

# tcd threshold mapping
thresh_to_pct = {
    0: "0",
    1: "10",
    2: "15",
    3: "20",
    4: "25",
    5: "30",
    6: "50",
    7: "75",
}


class TreeCoverLossTasks:
    def __init__(
        self,
        qc_feature_repository=QCFeaturesRepository(),
        gee_repository=GoogleEarthEngineDatasetRepository(),
        qc_error_threshold=0.01,
    ):
        self.qc_feature_repository = qc_feature_repository
        self.gee_repository = gee_repository
        self.qc_error_threshold = qc_error_threshold

    def load_data(
        self,
        tree_cover_loss_uri: str,
        pixel_area_uri: Optional[str] = None,
        carbon_emissions_uri: Optional[str] = None,
        tree_cover_density_uri: Optional[str] = None,
        ifl_uri: Optional[str] = None,
        drivers_uri: Optional[str] = None,
        primary_forests_uri: Optional[str] = None,
        natural_forests_uri: Optional[str] = None,
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

        natural_forests: xr.DataArray = _load_zarr(natural_forests_uri).band_data
        natural_forests = xr.align(
            tcl,
            natural_forests.reindex_like(tcl, method="nearest", tolerance=1e-5),
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
            natural_forests,
            country,
            region,
            subregion,
        )

    def compute_zonal_stat(self, *args, **kwargs) -> xr.DataArray:
        return common_stages.compute(*args, **kwargs)

    def setup_compute(
        self,
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
            natural_forests,
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
            natural_forests.rename("natural_forest_class"),
            country.rename("country"),
            region.rename("region"),
            subregion.rename("subregion"),
        )

        return (mask, groupbys, expected_groups)

    def create_result_dataframe(self, result: xr.DataArray) -> pd.DataFrame:
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

    def postprocess_result(self, result: xr.DataArray) -> pd.DataFrame:
        result_df = self.create_result_dataframe(result)
        # convert year values (1-24) to actual years (2001-2024)

        result_df["tree_cover_loss_year"] = result_df["tree_cover_loss_year"] + 2000

        # convert tcl thresholds to percentages
        result_df["canopy_cover"] = result_df["canopy_cover"].map(thresh_to_pct)

        # convert ifl to boolean
        result_df["is_intact_forest"] = result_df["is_intact_forest"].astype(bool)

        # convert driver codes to labels
        categoryid_to_driver = {
            1: "Permanent agriculture",
            2: "Hard commodities",
            3: "Shifting cultivation",
            4: "Logging",
            5: "Wildfire",
            6: "Settlements and infrastructure",
            7: "Other natural disturbances",
        }

        result_df["driver"] = result_df["driver"].map(categoryid_to_driver)

        # convert primary forest to boolean
        result_df["is_primary_forest"] = result_df["is_primary_forest"].astype(bool)

        natural_forest_class_to_label = {
            0: "Unknown",
            1: "Natural Forest",
            2: "Non-natural Forest",
        }
        result_df["natural_forest_class"] = result_df["natural_forest_class"].map(
            natural_forest_class_to_label
        )

        result_df["country"] = result_df["country"].map(numeric_to_alpha3)
        result_df.dropna(subset=["country"], inplace=True)

        return result_df

    def qc_against_validation_source(self):
        qc_features = self.qc_feature_repository.load()

        def qc_feature(row):
            sample_stats = self.get_sample_statistics(row.geometry)
            iso, adm1_str, adm2_suffix = row.GID_2.split(".")
            adm2_str, _ = adm2_suffix.split("_")

            adm1 = int(adm1_str)
            adm2 = int(adm2_str)

            if sample_stats.size > 0:
                sample_driver_area_ha_total = sample_stats[
                    (sample_stats.canopy_cover.astype(np.int8) >= 30)
                    & ~(sample_stats.driver.isna())
                    & (sample_stats.country == iso)
                    & (sample_stats.region == adm1)
                    & (sample_stats.subregion == adm2)
                ].area_ha.sum()

                sample_natural_forests_ha_total = sample_stats[
                    (sample_stats.natural_forest_class != "Unknown")
                    & (sample_stats.tree_cover_loss_year > 2020)
                    & (sample_stats.country == iso)
                    & (sample_stats.region == adm1)
                    & (sample_stats.subregion == adm2)
                ].area_ha.sum()
            else:
                sample_driver_area_ha_total = 0
                sample_natural_forests_ha_total = 0

            validation_stats = self.get_validation_statistics(row.geometry)
            if validation_stats["driver_results"].size > 0:
                validation_driver_area_ha_total = validation_stats[
                    "driver_results"
                ].area_ha.sum()
            else:
                validation_driver_area_ha_total = 0

            if validation_stats["natural_forests_results"].size > 0:
                validation_natural_forests_ha_total = validation_stats[
                    "natural_forests_results"
                ].area_ha.sum()
            else:
                validation_natural_forests_ha_total = 0

            diff_driver = abs(
                (validation_driver_area_ha_total - sample_driver_area_ha_total)
                / validation_driver_area_ha_total
            )
            diff_natural_forest = abs(
                (validation_natural_forests_ha_total - sample_natural_forests_ha_total)
                / validation_natural_forests_ha_total
            )

            driver_result = bool(diff_driver < self.qc_error_threshold)
            natural_forest_result = bool(diff_natural_forest < self.qc_error_threshold)

            return pd.Series(
                {
                    "pass": all([driver_result, natural_forest_result]),
                    "sample_driver": sample_driver_area_ha_total,
                    "validation_driver": validation_driver_area_ha_total,
                    "sample_natural_forest": sample_natural_forests_ha_total,
                    "validation_natural_forest": validation_natural_forests_ha_total,
                    "detail": "",
                }
            )

        qc_features[
            [
                "qc_pass",
                "sample_driver",
                "validation_driver",
                "sample_natural_forest",
                "validation_natural_forest",
                "detail",
            ]
        ] = qc_features.apply(qc_feature, axis=1)

        # qc_features.to_file("validation_results.geojson", index=False)
        return bool(qc_features.qc_pass.all())

    def get_sample_statistics(self, geom: Polygon) -> pd.DataFrame:
        results = compute_tree_cover_loss(self, bbox=geom)
        return results

    def get_validation_statistics(
        self,
        geom: Polygon,
    ) -> Dict[str, pd.DataFrame]:
        loss_ds = self.gee_repository.load("loss", geom)

        # pull only what we need
        loss = loss_ds.loss  # 0/1
        tcd = loss_ds.treecover2000  # 0-100
        loss_year = loss_ds.lossyear

        loss_mask = loss == 1
        loss_tcd30_mask = loss_mask & (tcd > 30)

        drivers_ds = self.gee_repository.load("tcl_drivers", geom, like=loss)
        drivers_class = drivers_ds.classification.where(loss_tcd30_mask)

        natural_lands_class = self.gee_repository.load(
            "natural_lands", geom, like=loss
        ).classification
        natural_forests_class = xr.where(
            natural_lands_class.isin([2, 5, 8, 9]),
            1,
            xr.where(natural_lands_class.isin([14, 17, 18]), 2, 0),
        )

        area = self.gee_repository.load("area", geom, like=loss) / 10000

        # if the whole thing is masked just exit early
        if loss_tcd30_mask.isnull().all().item() or drivers_class.isnull().all().item():
            driver_results = pd.DataFrame({"area_ha": [], "driver": []})
        else:
            driver_results = (
                area.groupby(drivers_class)
                .sum(skipna=True)
                .to_dataframe()
                .reset_index()
            )
            driver_results = driver_results.rename(
                columns={"area": "area_ha", "classification": "driver"}
            )

        natural_forests_results = (
            area.where(loss_year > 20)
            .where(natural_forests_class > 0)
            .groupby(natural_forests_class)
            .sum()
            .to_dataframe()
            .reset_index()
        )
        natural_forests_results = natural_forests_results.rename(
            columns={"area": "area_ha", "classification": "natural_forests_class"}
        )

        return {
            "driver_results": driver_results,
            "natural_forests_results": natural_forests_results,
        }

    def get_validation_statistics_old(
        self,
        geom: Polygon,
    ) -> pd.DataFrame:
        geom_ee = ee.Geometry(mapping(geom))

        gfc = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
        loss = gfc.select("loss").selfMask()
        tree_cover = gfc.select("treecover2000")

        threshold_mask = tree_cover.gt(30)
        loss_tcd30 = loss.updateMask(threshold_mask)

        drivers24 = ee.Image(
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
            ee.Image(1)
            .addBands([permag, hard, shifting, logging, wildfire, settlements, natural])
            .rename(["blank"] + bands)
            .select(bands)
        )

        proj_info = gfc.projection().getInfo()
        crs = proj_info["crs"]
        transform = proj_info["transform"]

        def get_regional_stats(im, region):
            area_ha_img = im.multiply(ee.Image.pixelArea().divide(10000))
            stats = area_ha_img.reduceRegion(
                reducer=ee.Reducer.sum().unweighted(),
                geometry=region.geometry(),
                crs=crs,
                crsTransform=transform,
                bestEffort=False,
                maxPixels=1e12,
                tileScale=16,
            )
            return ee.Feature(None, stats).copyProperties(
                region, region.propertyNames()
            )

        adm2 = ee.FeatureCollection([ee.Feature(geom_ee, {"id": "aoi_1"})])
        fc = adm2.map(lambda f: get_regional_stats(drivers, ee.Feature(f)))

        # Bring it back to the client as JSON (Python dict)
        return fc.getInfo()
