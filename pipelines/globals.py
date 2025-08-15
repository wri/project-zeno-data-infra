DATA_LAKE_BUCKET = "gfw-data-lake"
GADM_VERSION = "v4.1.85"

country_zarr_uri = f"s3://{DATA_LAKE_BUCKET}/gadm_administrative_boundaries/{GADM_VERSION}/raster/epsg-4326/zarr/adm0.zarr"
region_zarr_uri = f"s3://{DATA_LAKE_BUCKET}/gadm_administrative_boundaries/{GADM_VERSION}/raster/epsg-4326/zarr/adm1.zarr"
subregion_zarr_uri = f"s3://{DATA_LAKE_BUCKET}/gadm_administrative_boundaries/{GADM_VERSION}/raster/epsg-4326/zarr/adm2.zarr"
pixel_area_uri = "s3://{DATA_LAKE_BUCKET}/umd_area_2013/v1.10/raster/epsg-4326/zarr/pixel_area.zarr"
