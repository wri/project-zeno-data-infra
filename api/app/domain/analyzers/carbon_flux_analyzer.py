from functools import partial
from typing import Dict

import dask.dataframe as dd
import newrelic.agent as nr_agent
import xarray as xr

from app.analysis.common.analysis import get_geojson, read_zarr_clipped_to_geojson
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.dataset import Dataset
from app.domain.models.environment import Environment
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.models.land_change.carbon_flux import CarbonFluxAnalyticsIn

INPUT_URIS = {
    Environment.test: {
        "admin_results_uri": "s3://lcl-analytics/zonal-statistics/admin-carbon-flux.parquet"
    },
    Environment.staging: {
        "carbon_net_flux_zarr_uri": "s3://lcl-analytics/zarr/gfw-carbon-net-flux/v20260327/Mg_CO2e.zarr/",
        "carbon_gross_removals_zarr_uri": "s3://lcl-analytics/zarr/gfw-carbon-gross-removals/v20260327/Mg_CO2e.zarr/",
        "carbon_gross_emissions_zarr_uri": "s3://lcl-analytics/zarr/gfw-carbon-gross-emissions/v20260327/Mg_CO2e.zarr/",
        "mangrove_stock_2000_zarr_uri": "s3://gfw-data-lake/jpl_mangrove_aboveground_biomass_stock_2000/v201902/raster/epsg-4326/zarr/is_mangrove.zarr/",
        # Value 1, 2, 3, 4, which means 2001, 2005, 2010, 2015 periods
        "tree_cover_gain_from_height_zarr_uri": "s3://gfw-data-lake/umd_tree_cover_gain_from_height/v20240126/raster/epsg-4326/zarr/period.zarr/",
        # Value [0, 100] inclusive.
        "tree_cover_density_2000_zarr_uri": "s3://gfw-data-lake/umd_tree_cover_density_2000/v1.8/raster/epsg-4326/zarr/threshold.zarr/",
        "admin_results_uri": "s3://lcl-analytics/zonal-statistics/admin-carbon.parquet",
    },
    Environment.production: {
        # These are zarrs with total emissions (not per-hectare)
        "carbon_net_flux_zarr_uri": "s3://gfw-data-lake/gfw_forest_carbon_net_flux/v20250430/raster/epsg-4326/zarr/Mg_CO2e.zarr/",
        "carbon_gross_removals_zarr_uri": "s3://gfw-data-lake/gfw_forest_carbon_gross_removals/v20250416/raster/epsg-4326/zarr/Mg_CO2e.zarr/",
        "carbon_gross_emissions_zarr_uri": "s3://gfw-data-lake/gfw_forest_carbon_gross_emissions/v20250430/raster/epsg-4326/zarr/Mg_CO2e.zarr/",
        # Boolean value (0, 1)
        "mangrove_stock_2000_zarr_uri": "s3://gfw-data-lake/jpl_mangrove_aboveground_biomass_stock_2000/v201902/raster/epsg-4326/zarr/is_mangrove.zarr/",
        # Value 1, 2, 3, 4, which means 2001, 2005, 2010, 2015 periods
        "tree_cover_gain_from_height_zarr_uri": "s3://gfw-data-lake/umd_tree_cover_gain_from_height/v20240126/raster/epsg-4326/zarr/period.zarr/",
        # Value [0, 100] inclusive.
        "tree_cover_density_2000_zarr_uri": "s3://gfw-data-lake/umd_tree_cover_density_2000/v1.8/raster/epsg-4326/zarr/threshold.zarr/",
<<<<<<< tcl-v1.13-pipeline
=======
        # Value [1,24] inclusive
        "tree_cover_loss_zarr_uri": "s3://gfw-data-lake/umd_tree_cover_loss/v1.12/raster/epsg-4326/zarr/year.zarr/",
>>>>>>> main
        # This is a simpler parquet with just country, region, subregion, tree_cover_density,
        # carbontype, value columns. Use equality on the tree_cover_density column, which has
        # values 30/50/75...
        "admin_results_uri": "s3://lcl-analytics/zonal-statistics/admin-carbon2.parquet",
        # "admin_results_uri": "s3://gfw-data-lake/gfw_forest_carbon_net_flux/v20250430/tabular/zonal_stats/gadm/gadm_adm2.parquet"
    },
}


class CarbonFluxAnalyzer(Analyzer):
    """Get the carbon emissions, removal, flux for 2000-2024 for different canopy densities"""

    def __init__(
        self,
        compute_engine=None,
        query_service=None,
        input_uris: Dict[str, str] | None = None,
    ):
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.query_service = query_service
        self.input_uris = input_uris

    @nr_agent.function_trace(name="CarbonFluxAnalyzer.analyze")
    async def analyze(self, analysis: Analysis) -> None:
        if self.input_uris is None:
            raise RuntimeError("Input URIs must be provided for actual analysis")

        carbon_flux_analytics_in = CarbonFluxAnalyticsIn(**analysis.metadata)

        if carbon_flux_analytics_in.aoi.type == "admin":
            gadm_ids = carbon_flux_analytics_in.aoi.ids
            results = await self.analyze_admin_areas(
                gadm_ids, carbon_flux_analytics_in.canopy_cover
            )
        else:
            aois = carbon_flux_analytics_in.aoi.model_dump()
            geojsons = await get_geojson(aois)
            if aois["type"] != "feature_collection":
                aoi_list = sorted(
                    [{"type": aois["type"], "id": id} for id in aois["ids"]],
                    key=lambda aoi: aoi["id"],
                )
            else:
                aoi_list = aois["feature_collection"]["features"]
                geojsons = [geojson["geometry"] for geojson in geojsons]

            analysis_partial = partial(
                self.analyze_area,
                self.input_uris,
                threshold=carbon_flux_analytics_in.canopy_cover,
            )
            dd_df_futures = await self.compute_engine.gather(
                self.compute_engine.map(analysis_partial, aoi_list, geojsons)
            )
            dfs = await self.compute_engine.gather(dd_df_futures)
            results = await self.compute_engine.compute(dd.concat(dfs))
            results = results.to_dict(orient="list")

        analysis.result = results

    async def analyze_admin_areas(self, gadm_ids, threshold=30) -> Dict:
        if self.query_service is None:
            raise RuntimeError("query_service must be set to call this function")

        id_str = (", ").join([f"'{aoi_id}'" for aoi_id in gadm_ids])
        query = f"select aoi_id, carbon_net_flux_Mg_CO2e, carbon_gross_removals_Mg_CO2e, carbon_gross_emissions_Mg_CO2e from data_source where aoi_id in ({id_str}) and tree_cover_density = {threshold}"

        results_dict = await self.query_service.execute(query)
        results_dict["aoi_type"] = ["admin"] * len(results_dict["aoi_id"])

        return results_dict

    @staticmethod
    def analyze_area(
        input_uris: Dict[str, str], aoi, geojson, threshold=30
    ) -> dd.DataFrame:
        threshold_pixel_value = ZarrDatasetRepository().translate(
            Dataset.canopy_cover, threshold
        )

        carbon_net_flux = read_zarr_clipped_to_geojson(
            input_uris["carbon_net_flux_zarr_uri"], geojson
        )

        carbon_gross_removals = read_zarr_clipped_to_geojson(
            input_uris["carbon_gross_removals_zarr_uri"], geojson
        )

        carbon_gross_emissions = read_zarr_clipped_to_geojson(
            input_uris["carbon_gross_emissions_zarr_uri"], geojson
        )

        mangrove_stock_2000 = read_zarr_clipped_to_geojson(
            input_uris["mangrove_stock_2000_zarr_uri"], geojson
        ).band_data
        mangrove_stock_2000.name = "is_mangrove_stock_2000"

        tree_cover_gain_from_height = read_zarr_clipped_to_geojson(
            input_uris["tree_cover_gain_from_height_zarr_uri"], geojson
        ).band_data
        tree_cover_gain_from_height.name = "tree_cover_gain_from_height"

        tree_cover_density_2000 = read_zarr_clipped_to_geojson(
            input_uris["tree_cover_density_2000_zarr_uri"], geojson
        ).band_data
        tree_cover_density_2000.name = "tree_cover_density_2000"

        ds = xr.Dataset(
            {
                "carbon_gross_removals_Mg_CO2e": carbon_gross_removals.band_data,
                "carbon_net_flux_Mg_CO2e": carbon_net_flux.band_data,
            }
        )

        merge = ds * (
            (tree_cover_density_2000 >= threshold_pixel_value)
            | mangrove_stock_2000
            | tree_cover_gain_from_height
            > 0
        )
        emissions = carbon_gross_emissions.band_data * (
            tree_cover_density_2000 >= threshold_pixel_value
        )

        merge["carbon_gross_emissions_Mg_CO2e"] = emissions

        carbon_df: dd.DataFrame = (
            merge.sum(dim=("x", "y"))
            .to_dask_dataframe()
            .drop("spatial_ref", axis=1)
            .drop("band", axis=1)
        )

        carbon_df["aoi_type"] = aoi["type"].lower()
        carbon_df["aoi_id"] = aoi["id"] if "id" in aoi else aoi["properties"]["id"]

        return carbon_df
