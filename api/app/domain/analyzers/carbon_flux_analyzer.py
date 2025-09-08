from functools import partial

import dask.dataframe as dd
import duckdb
import xarray as xr
from app.analysis.common.analysis import (
    get_geojson,
    initialize_duckdb,
    read_zarr_clipped_to_geojson,
)
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.dataset import Dataset
from app.domain.repositories.zarr_dataset_repository import ZarrDatasetRepository
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.carbon_flux import CarbonFluxAnalyticsIn

# These are zarrs with total emissions (not per-hectare)
carbon_net_flux_zarr_uri = "s3://gfw-data-lake/gfw_forest_carbon_net_flux/v20250430/raster/epsg-4326/zarr/Mg_CO2e.zarr/"
carbon_gross_removals_zarr_uri = "s3://gfw-data-lake/gfw_forest_carbon_gross_removals/v20250416/raster/epsg-4326/zarr/Mg_CO2e.zarr/"
carbon_gross_emissions_zarr_uri = "s3://gfw-data-lake/gfw_forest_carbon_gross_emissions/v20250430/raster/epsg-4326/zarr/Mg_CO2e.zarr/"

# Boolean value (0, 1)
mangrove_stock_2000_zarr_uri = "s3://gfw-data-lake/jpl_mangrove_aboveground_biomass_stock_2000/v201902/raster/epsg-4326/zarr/is_mangrove.zarr/"

# Value 1, 2, 3, 4, which means 2001, 2005, 2010, 2015 periods
tree_cover_gain_from_height_zarr_uri = "s3://gfw-data-lake/umd_tree_cover_gain_from_height/v20240126/raster/epsg-4326/zarr/period.zarr/"
# Value [0, 100] inclusive.
tree_cover_density_2000_zarr_uri = "s3://gfw-data-lake/umd_tree_cover_density_2000/v1.8/raster/epsg-4326/zarr/threshold.zarr/"
# Value [1,24] inclusive
tree_cover_loss_zarr_uri = (
    "s3://gfw-data-lake/umd_tree_cover_loss/v1.12/raster/epsg-4326/zarr/year.zarr/"
)
# Parquet location
# admin_results_uri = "s3://gfw-data-lake/gfw_forest_carbon_net_flux/v20250430/tabular/zonal_stats/gadm/gadm_adm2.parquet"
admin_results_uri = "s3://lcl-analytics/zonal-statistics/admin-carbon.parquet"


def create_gadm_carbon_query(type, gadm_list, threshold):
    query = f"(select sum(value) from '{admin_results_uri}' where carbontype == '{type}' and country = '{gadm_list[0]}'"
    if len(gadm_list) > 1:
        query += f" AND region = {gadm_list[1]}"
    if len(gadm_list) > 2:
        query += f" AND subregion = {gadm_list[2]}"

    # emissions should filter by just TCD, but net flux/removals should filter by
    # TCD, gain and mangroves
    threshold_pixel_value = ZarrDatasetRepository().translate(
        Dataset.canopy_cover, threshold
    )
    if type == "carbon_gross_emissions":
        query += f"AND tree_cover_density >= {threshold_pixel_value}) AS {type}"
    else:
        query += f" AND (tree_cover_density >= {threshold_pixel_value} or mangrove_stock_2000 > 0 OR tree_cover_gain_from_height > 0)) AS {type}"
    return query


class CarbonFluxAnalyzer(Analyzer):
    """Get the carbon emissions, removal, flux for 2000-2024 for different canopy densitys"""

    def __init__(
        self,
        analysis_repository=None,
        compute_engine=None,
        dataset_repository=None,
    ):
        self.analysis_repository = analysis_repository  # CarbonFluxRepository
        self.compute_engine = compute_engine  # Dask Client, or not?
        self.dataset_repository = dataset_repository  # AWS-S3 for zarrs, etc.

    async def analyze(self, analysis: Analysis):
        carbon_flux_analytics_in = CarbonFluxAnalyticsIn(**analysis.metadata)
        if carbon_flux_analytics_in.aoi.type == "admin":
            analysis_partial = partial(
                self.analyze_admin_area, threshold=carbon_flux_analytics_in.canopy_cover
            )
            dd_df_futures = await self.compute_engine.gather(
                self.compute_engine.map(
                    analysis_partial, carbon_flux_analytics_in.aoi.ids
                )
            )
            dfs = await self.compute_engine.gather(dd_df_futures)
            combined_results_df = await self.compute_engine.compute(dd.concat(dfs))

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
                self.analyze_area, threshold=carbon_flux_analytics_in.canopy_cover
            )
            dd_df_futures = await self.compute_engine.gather(
                self.compute_engine.map(analysis_partial, aoi_list, geojsons)
            )
            dfs = await self.compute_engine.gather(dd_df_futures)
            combined_results_df = await self.compute_engine.compute(dd.concat(dfs))

        analyzed_analysis = Analysis(
            combined_results_df.to_dict(orient="list"),
            analysis.metadata,
            AnalysisStatus.saved,
        )
        await self.analysis_repository.store_analysis(
            carbon_flux_analytics_in.thumbprint(), analyzed_analysis
        )

    @staticmethod
    def analyze_admin_area(gadm_id, threshold=30) -> dd.DataFrame:
        gadm_list = gadm_id.split(".")
        query = "select "
        query += (
            create_gadm_carbon_query("carbon_net_flux", gadm_list, threshold) + ", "
        )
        query += (
            create_gadm_carbon_query("carbon_gross_removals", gadm_list, threshold)
            + ", "
        )
        query += create_gadm_carbon_query(
            "carbon_gross_emissions", gadm_list, threshold
        )

        initialize_duckdb()
        df = duckdb.query(query).df()
        df["aoi_id"] = gadm_id
        df["aoi_type"] = "admin"
        df.rename(
            columns={
                "carbon_net_flux": "carbon_net_flux_Mg_CO2e",
                "carbon_gross_removals": "carbon_gross_removals_Mg_CO2e",
                "carbon_gross_emissions": "carbon_gross_emissions_Mg_CO2e",
            },
            inplace=True,
        )

        # Return a dask data frame, so the future gather code works.
        ddf: dd.DataFrame = dd.from_pandas(df)
        return ddf

    @staticmethod
    def analyze_area(aoi, geojson, threshold=30) -> dd.DataFrame:
        threshold_pixel_value = ZarrDatasetRepository().translate(
            Dataset.canopy_cover, threshold
        )
        carbon_net_flux = read_zarr_clipped_to_geojson(
            carbon_net_flux_zarr_uri, geojson
        )
        carbon_gross_removals = read_zarr_clipped_to_geojson(
            carbon_gross_removals_zarr_uri, geojson
        )
        carbon_gross_emissions = read_zarr_clipped_to_geojson(
            carbon_gross_emissions_zarr_uri, geojson
        )
        mangrove_stock_2000 = read_zarr_clipped_to_geojson(
            mangrove_stock_2000_zarr_uri, geojson
        ).band_data
        mangrove_stock_2000.name = "is_mangrove_stock_2000"
        tree_cover_gain_from_height = read_zarr_clipped_to_geojson(
            tree_cover_gain_from_height_zarr_uri, geojson
        ).band_data
        tree_cover_gain_from_height.name = "tree_cover_gain_from_height"
        tree_cover_density_2000 = read_zarr_clipped_to_geojson(
            tree_cover_density_2000_zarr_uri, geojson
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
