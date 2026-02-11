import base64
import json
import os

import ee

DATA_LAKE_BUCKET = "gfw-data-lake"
ANALYTICS_BUCKET = "lcl-analytics"
GADM_VERSION = "v4.1.85"

GEE_SERVICE_ACCOUNT_FILE = "gee_credentials.json"
if os.getenv("GEE_SERVICE_ACCOUNT_JSON") is not None:
    gee_service_account_b64 = os.environ["GEE_SERVICE_ACCOUNT_JSON"]
    GEE_SERVICE_ACCOUNT_JSON = json.loads(
        base64.b64decode(gee_service_account_b64).decode("utf-8")
    )
    with open(GEE_SERVICE_ACCOUNT_FILE, "w") as f:
        json.dump(GEE_SERVICE_ACCOUNT_JSON, f)

    creds = ee.ServiceAccountCredentials(
        GEE_SERVICE_ACCOUNT_JSON["client_email"],
        GEE_SERVICE_ACCOUNT_FILE,
    )

    ee.Initialize()

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
tree_cover_loss_zarr_uri = (
    f"s3://{ANALYTICS_BUCKET}/zarr/umd-tree-cover-loss/v1.12/year.zarr"
)
tree_cover_density_zarr_uri = (
    f"s3://{ANALYTICS_BUCKET}/zarr/umd_tree_cover_density_2000/v1.8/threshold.zarr"
)
carbon_emissions_zarr_uri = (
    f"s3://{ANALYTICS_BUCKET}/zarr/gfw-carbon-emissions/v20250430/Mg_CO2e_float64.zarr"
)
wri_google_1km_drivers_zarr_uri = "s3://gfw-data-lake/wri_google_tree_cover_loss_drivers/v1.12/raster/epsg-4326/zarr/category.zarr"
ifl_intact_forest_lands_zarr_uri = (
    f"s3://{ANALYTICS_BUCKET}/zarr/ifl-intact-forest-landscapes-2000/v2021/is.zarr/"
)
umd_primary_forests_zarr_uri = (
    f"s3://{ANALYTICS_BUCKET}/zarr/umd-regional-primary-forest-2001/v201901/is.zarr/"
)
