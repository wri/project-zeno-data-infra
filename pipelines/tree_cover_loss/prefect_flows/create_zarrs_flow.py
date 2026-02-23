from prefect import flow

from pipelines.globals import (
    DATA_LAKE_BUCKET,
    carbon_emissions_zarr_uri,
    ifl_intact_forest_lands_zarr_uri,
    pixel_area_zarr_uri,
    tree_cover_density_zarr_uri,
    tree_cover_loss_zarr_uri,
    umd_primary_forests_zarr_uri,
    wri_google_1km_drivers_zarr_uri,
)
from pipelines.prefect_flows import common_tasks

# -- Source tile manifest URIs (tiled GeoTIFFs) --
_TCL_SOURCE = f"s3://{DATA_LAKE_BUCKET}/umd_tree_cover_loss/v1.12/raster/epsg-4326/10/40000/year/gdal-geotiff/tiles.geojson"
_TCD_SOURCE = f"s3://{DATA_LAKE_BUCKET}/umd_tree_cover_density_2000/v1.8/raster/epsg-4326/10/40000/threshold/gdal-geotiff/tiles.geojson"
_EMISSIONS_SOURCE = f"s3://{DATA_LAKE_BUCKET}/gfw_forest_carbon_gross_emissions/v20250430/raster/epsg-4326/10/40000/Mg_CO2e_ha-1/gdal-geotiff/tiles.geojson"
_DRIVERS_SOURCE = f"s3://{DATA_LAKE_BUCKET}/wri_google_tree_cover_loss_drivers/v1.12/raster/epsg-4326/10/40000/category/gdal-geotiff/tiles.geojson"
_IFL_SOURCE = f"s3://{DATA_LAKE_BUCKET}/ifl_intact_forest_landscapes_2000/v2021/raster/epsg-4326/10/100000/is/gdal-geotiff/tiles.geojson"
_PRIMARY_SOURCE = f"s3://{DATA_LAKE_BUCKET}/umd_regional_primary_forest_2001/v201901/raster/epsg-4326/10/40000/is/gdal-geotiff/tiles.geojson"
_PIXEL_AREA_SOURCE = f"s3://{DATA_LAKE_BUCKET}/umd_area_2013/v1.10/raster/epsg-4326/10/40000/area_m/gdal-geotiff/tiles.geojson"


def _to_carbon_emissions(dataset):
    """Rename band_data to carbon_emissions_MgCO2e and cast to float64."""
    return dataset.rename("carbon_emissions_MgCO2e").astype("float64")


@flow(name="Create zarrs")
def create_zarrs(overwrite: bool = False):
    """Create all zarr stores needed by the TCL pipeline."""
    common_tasks.create_zarr.with_options(name="create-tcl-zarr")(
        _TCL_SOURCE,
        tree_cover_loss_zarr_uri,
        overwrite=overwrite,
    )
    common_tasks.create_zarr.with_options(name="create-carbon-emissions-zarr")(
        _EMISSIONS_SOURCE,
        carbon_emissions_zarr_uri,
        transform=_to_carbon_emissions,
        overwrite=overwrite,
    )
    common_tasks.create_zarr.with_options(name="create-tcd-zarr")(
        _TCD_SOURCE,
        tree_cover_density_zarr_uri,
        overwrite=overwrite,
    )
    common_tasks.create_zarr.with_options(name="create-ifl-zarr")(
        _IFL_SOURCE,
        ifl_intact_forest_lands_zarr_uri,
        overwrite=overwrite,
    )
    common_tasks.create_zarr.with_options(name="create-drivers-zarr")(
        _DRIVERS_SOURCE,
        wri_google_1km_drivers_zarr_uri,
        overwrite=overwrite,
    )
    common_tasks.create_zarr.with_options(name="create-primary-forests-zarr")(
        _PRIMARY_SOURCE,
        umd_primary_forests_zarr_uri,
        overwrite=overwrite,
    )
