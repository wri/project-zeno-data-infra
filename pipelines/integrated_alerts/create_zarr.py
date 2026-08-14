from datetime import datetime, timedelta

import numpy as np
import xarray as xr
import pandas as pd
import io
import boto3
import rasterio

from pipelines.globals import ANALYTICS_BUCKET, DATA_LAKE_BUCKET
from pipelines.integrated_alerts.register_gee_asset import register_gee_asset
from pipelines.integrated_alerts.transfer_to_gcs import transfer_s3_to_gcs
from pipelines.utils import copy_s3_directory, s3_uri_exists

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


def decode_alert_data(band_data) -> xr.Dataset:
    """Convert encoded alert date_conf data into separate confidence
    and alert date variables."""
    alert_date = (band_data % 10000).astype(np.uint16)
    alert_conf = (band_data // 10000).astype(np.uint8)
    alert_conf.name = "confidence"
    alert_date.name = "alert_date"
    return xr.merge((alert_conf, alert_date))


def _is_first_sunday_week(version: str) -> bool:
    """True if version (format 'vYYYYMMDD') falls within the Sunday-Saturday
    week that starts on the first Sunday of its month."""
    version_date = datetime.strptime(version, "v%Y%m%d").date()
    first_of_month = version_date.replace(day=1)
    days_until_sunday = (6 - first_of_month.weekday()) % 7  # weekday(): Mon=0..Sun=6
    first_sunday = first_of_month + timedelta(days=days_until_sunday)
    return first_sunday <= version_date <= first_sunday + timedelta(days=6)


def create_zarr(version, overwrite=False) -> tuple[str, bool]:
    """create a full extent zarr file in s3.

    Returns (zarr_uri, run_first_sunday_processing). The second value tells the
    caller whether it should also run first_sunday_processing(version, zarr_uri) for
    this version -- true only when this call actually built a fresh zarr and the
    version falls in the first-Sunday-of-month week.

    """
    base_folder = f"gfw_integrated_dist_alerts/{version}/raster/epsg-4326"
    # zarr_uri if we were going to write it back to gfw-data-lake
    # zarr_uri = f"s3://{DATA_LAKE_BUCKET}/{base_folder}/zarr/date_conf.zarr"

    zarr_uri = (
        f"s3://{ANALYTICS_BUCKET}/zarr/gfw_integrated_dist_alerts/{version}/date_conf.zarr"
    )

    if s3_uri_exists(f"{zarr_uri}/zarr.json") and not overwrite:
        return zarr_uri, False

    # Use get_object rather than pd.read_json(), so we can use RequestPayer config.
    # tiles = pd.read_json(tiles_uri)
    s3_client = boto3.client('s3')
    response = s3_client.get_object(
        Bucket=DATA_LAKE_BUCKET,
        Key=f"{base_folder}/10/100000/date_conf/geotiff/tiles.geojson",
        RequestPayer='requester'
    )
    tiles = pd.read_json(io.BytesIO(response['Body'].read()))

    tile_uris = tiles.features.apply(
        lambda x: "/".join(["s3:/"] + x["properties"]["name"].split("/")[2:])
    )

    # Add these extra config options to make sure the operation doesn't fail
    # due to transient "slow-down" returns from S3, because of all the parallel
    # requests to the same folder.
    with rasterio.Env(
            GDAL_HTTP_MAX_RETRY=5,              # Try up to 5 times before failing
            GDAL_HTTP_RETRY_DELAY=2,            # Wait 2 seconds before the first retry (uses exponential backoff)
            GDAL_HTTP_RETRY_CODES="429,500,502,503,504",  # Trigger retries on these HTTP codes
            GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR"  # Stop GDAL from listing S3 bucket looking for sidecar files
    ):
        print("Starting open_mfdataset")
        dataset = xr.open_mfdataset(
            tile_uris,
            parallel=True,
            engine="rasterio",
            chunks={"x": 10000, "y": 10000},
        )
        decoded_alert_data = decode_alert_data(dataset.band_data)
        print("Starting to_zarr")
        decoded_alert_data.to_zarr(zarr_uri, mode="w")
        print("Done to_zarr")

    return zarr_uri, _is_first_sunday_week(version)


def first_sunday_processing(version, zarr_uri) -> None:
    """First-Sunday-of-month-only extra processing for an int-dist version:
    copies the zarr to the monitoring bucket, registers the intdist_tropics COG as
    a GEE asset in forma-250, transfers that COG to GCS for landandcarbon,
    and registers it there too. Only called when create_zarr() returned
    run_first_sunday_processing=True for this version.
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
