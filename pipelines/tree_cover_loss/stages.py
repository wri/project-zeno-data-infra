from typing import Callable, Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import Polygon

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


class TreeCoverLossTasks:
    def __init__(
        self,
        qc_feature_repository=QCFeaturesRepository(),
        gee_repository=GoogleEarthEngineDatasetRepository(),
    ):
        self.qc_feature_repository = qc_feature_repository
        self.gee_repository = gee_repository

    def load_data(
        self,
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
        return self.create_result_dataframe(result)

    def qc_against_validation_source(self):
        qc_features = self.qc_feature_repository.load()

        def qc_feature(geom):
            sample_stats = self.get_sample_statistics(geom)
            validation_stats = self.get_validation_statistics(geom)
            diff = (
                validation_stats["area_ha"] - sample_stats["area_ha"]
            ) / validation_stats["area_ha"]

            if (diff.abs() > 0.01).any():
                return False

            return True

        qc_features["qc_pass"] = qc_features.geometry.apply(qc_feature)

        return qc_features.qc_pass.all()

    def get_sample_statistics(self, geom: Polygon) -> pd.DataFrame:

        results = compute_tree_cover_loss(self, bbox=geom)
        return results

    def get_validation_statistics(
        self,
        geom: Polygon,
    ):
        loss_ds = self.gee_repository.load("loss", geom)

        # pull only what we need
        loss = loss_ds.loss  # 0/1
        tcd = loss_ds.treecover2000  # 0-100

        loss_mask = loss == 1
        loss_tcd30_mask = loss_mask & (tcd > 30)

        drivers_ds = self.gee_repository.load("tcl_drivers", geom, like=loss)
        drivers_class = drivers_ds.classification.where(loss_tcd30_mask)

        area = self.gee_repository.load("area", geom, like=loss) / 10000

        results = (
            area.groupby(drivers_class).sum(skipna=True).to_dataframe().reset_index()
        )
        results = results.rename(
            columns={"area": "area_ha", "classification": "driver"}
        )
        return results
