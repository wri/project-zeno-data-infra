import numpy as np
import xarray as xr
import pandas as pd
import io
import boto3
import rasterio

from pipelines.globals import ANALYTICS_BUCKET, DATA_LAKE_BUCKET
from pipelines.utils import s3_uri_exists


def decode_alert_data(band_data) -> xr.Dataset:
    """Convert encoded alert date_conf data into separate confidence
    and alert date variables."""
    alert_date = (band_data % 10000).astype(np.uint16)
    alert_conf = (band_data // 10000).astype(np.uint8)
    alert_conf.name = "confidence"
    alert_date.name = "alert_date"
    return xr.merge((alert_conf, alert_date))


def create_zarr(version, overwrite=False) -> str:
    """create a full extent zarr file in s3."""
    base_folder = f"gfw_integrated_dist_alerts/{version}/raster/epsg-4326"
    # zarr_uri if we were going to write it back to gfw-data-lake
    # zarr_uri = f"s3://{DATA_LAKE_BUCKET}/{base_folder}/zarr/date_conf.zarr"

    zarr_uri = (
        f"s3://{ANALYTICS_BUCKET}/zarr/gfw_integrated_dist_alerts/{version}/date_conf.zarr"
    )

    if s3_uri_exists(f"{zarr_uri}/zarr.json") and not overwrite:
        return zarr_uri

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

    return zarr_uri
