DATA_LAKE_BUCKET = "gfw-data-lake"
ANALYTICS_BUCKET = "lcl-analytics"
GADM_VERSION = "v4.1.85"

country_zarr_uri = f"s3://{ANALYTICS_BUCKET}/zarr/gadm-administrative-boundaries/{GADM_VERSION}/adm0.zarr"
region_zarr_uri = f"s3://{ANALYTICS_BUCKET}/zarr/gadm-administrative-boundaries/{GADM_VERSION}/adm1.zarr"
subregion_zarr_uri = f"s3://{ANALYTICS_BUCKET}/zarr/gadm-administrative-boundaries/{GADM_VERSION}/adm2.zarr"
pixel_area_zarr_uri = (
    f"s3://{ANALYTICS_BUCKET}/zarr/umd-area-2013/v1.10/pixel_area_ha.zarr"
)
grasslands_zarr_uri = f"s3://{ANALYTICS_BUCKET}/zarr/grasslands/v1/grasslands.zarr"
sbtn_natural_lands_zarr_uri = f"s3://{ANALYTICS_BUCKET}/zarr/sbtn-natural-lands/sbtn_natural_lands_all_classes.zarr"
dist_driver_zarr_uri = (
    f"s3://{ANALYTICS_BUCKET}/zarr/dist-alerts-drivers/umd_dist_alerts_drivers.zarr"
)

land_cover_zarr_uri = (
    f"s3://{ANALYTICS_BUCKET}/zarr/land-cover/umd_lcl_land_cover_2015-2024.zarr"
)
