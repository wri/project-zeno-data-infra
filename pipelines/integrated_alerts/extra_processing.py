from datetime import datetime, timedelta

from pipelines.globals import DATA_LAKE_BUCKET
from pipelines.integrated_alerts.register_gee_asset import register_gee_asset
from pipelines.integrated_alerts.transfer_to_gcs import transfer_s3_to_gcs
from pipelines.utils import copy_s3_directory

MONITORING_BUCKET = "gnw-monitoring-data"

# The tropics COG is registered as a GEE asset twice, in two different GEE
# projects, from two different GCS copies of the same file: forma-250's copy
# is uploaded upstream (outside this pipeline) to data-api-gee-assets;
# landandcarbon's copy is the one this pipeline itself transfers from S3
# below, landing at INTDIST_TROPICS_LANDANDCARBON_GCS_URI.
INTDIST_TROPICS_FORMA250_GCS_URI = (
    "gs://data-api-gee-assets/gfw_integrated_dist_alerts/intdist_tropics.tif"
)
INTDIST_TROPICS_FORMA250_GEE_ASSET_PATH = "gfw_integrated_dist_alerts/intdist_tropics"

INTDIST_TROPICS_S3_SOURCE_SECRET_ID = "s3-auth/gfw-data-lake-readonly"
INTDIST_TROPICS_LANDANDCARBON_GCS_FOLDER_URI = "gs://wri-lcl-integrated-alerts/"
INTDIST_TROPICS_LANDANDCARBON_GCS_URI = f"{INTDIST_TROPICS_LANDANDCARBON_GCS_FOLDER_URI}intdist_tropics.tif"
INTDIST_TROPICS_LANDANDCARBON_GEE_ASSET_PATH = "integrated_dist_alerts/intdist_tropics"


def is_first_sunday_week(version: str) -> bool:
    """True if version (format 'vYYYYMMDD') falls within the Sunday-Saturday
    week that starts on the first Sunday of its month."""
    version_date = datetime.strptime(version, "v%Y%m%d").date()
    first_of_month = version_date.replace(day=1)
    days_until_sunday = (6 - first_of_month.weekday()) % 7  # weekday(): Mon=0..Sun=6
    first_sunday = first_of_month + timedelta(days=days_until_sunday)
    return first_sunday <= version_date <= first_sunday + timedelta(days=6)


def extra_processing_tasks(version, zarr_uri) -> None:
    """First-Sunday-of-month-only extra processing for an int-dist version:
    copies the zarr to the monitoring bucket, registers the intdist_tropics COG as
    a GEE asset in forma-250, transfers that COG to GCS for landandcarbon,
    and registers it there too. Only call this when the version was
    freshly created (create_zarr()'s freshly_created) and
    is_first_sunday_week(version) is true.
    """
    base_folder = f"gfw_integrated_dist_alerts/{version}/raster/epsg-4326"

    monitoring_zarr_uri = (
        f"s3://{MONITORING_BUCKET}/zarrs/intdist.date_conf.{version}.zarr"
    )
    print(f"{version} is in the first-Sunday-of-month week; copying zarr to {monitoring_zarr_uri}")
    copy_s3_directory(zarr_uri, monitoring_zarr_uri)
    print("Done copying zarr")

    register_gee_asset(
        INTDIST_TROPICS_FORMA250_GCS_URI, INTDIST_TROPICS_FORMA250_GEE_ASSET_PATH,
        project="forma-250",
        force=True
    )
    print("Done registering GEE asset")

    source_cog_uri = f"s3://{DATA_LAKE_BUCKET}/{base_folder}/cog/"
    print(f"Transferring {source_cog_uri} to {INTDIST_TROPICS_LANDANDCARBON_GCS_FOLDER_URI}")
    transfer_s3_to_gcs(
        source_cog_uri,
        INTDIST_TROPICS_LANDANDCARBON_GCS_FOLDER_URI,
        INTDIST_TROPICS_S3_SOURCE_SECRET_ID,
        project="landandcarbon",
        include_prefixes=["intdist_tropics.tif"],
        timeout=7200
    )
    print("Done transferring to GCS")

    register_gee_asset(
        INTDIST_TROPICS_LANDANDCARBON_GCS_URI,
        INTDIST_TROPICS_LANDANDCARBON_GEE_ASSET_PATH,
        project="landandcarbon",
        force=True,
    )
    print("Done registering GEE asset for landandcarbon")
