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
sbtn_natural_forests_zarr_uri = (
    f"s3://{ANALYTICS_BUCKET}/zarr/sbtn-natural-forests/sbtn_natural_forests_class.zarr"
)
dist_driver_zarr_uri = (
    f"s3://{ANALYTICS_BUCKET}/zarr/dist-alerts-drivers/umd_dist_alerts_drivers.zarr"
)
land_cover_zarr_uri = (
    f"s3://{ANALYTICS_BUCKET}/zarr/land-cover/umd_lcl_land_cover_2015-2024.zarr"
)
tree_cover_density_2000_zarr_uri = (
    f"s3://{ANALYTICS_BUCKET}/zarr/umd_tree_cover_density_2000/v1.8/threshold.zarr"
)
ifl_intact_forest_lands_zarr_uri = (
    f"s3://{ANALYTICS_BUCKET}/zarr/ifl-intact-forest-landscapes-2000/v2021/is.zarr/"
)
umd_primary_forests_zarr_uri = (
    f"s3://{ANALYTICS_BUCKET}/zarr/umd-regional-primary-forest-2001/v201901/is.zarr/"
)
mangrove_stock_2000_zarr_uri = "s3://lcl-analytics/zarr/jpl_mangrove_aboveground_biomass_stock_2000/v201902/is_mangrove.zarr/"
tree_cover_gain_from_height_zarr_uri = "s3://lcl-analytics/zarr/umd_tree_cover_gain_from_height/v20240126/period.zarr/"
