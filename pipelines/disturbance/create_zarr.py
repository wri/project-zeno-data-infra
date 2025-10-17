import numpy as np
import xarray as xr

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
    base_folder = f"umd_glad_dist_alerts/{version}/raster/epsg-4326"
    zarr_uri = (
        f"s3://{ANALYTICS_BUCKET}/zarr/dist-alerts/{version}/umd_glad_dist_alerts.zarr"
    )
    cog_uri = f"s3://{DATA_LAKE_BUCKET}/{base_folder}/cog/default.tif"

    if s3_uri_exists(f"{zarr_uri}/zarr.json") and not overwrite:
        return zarr_uri

    dataset = xr.open_dataset(cog_uri, chunks="auto").band_data.chunk(
        {"x": 10000, "y": 10000}
    )
    decoded_alert_data = decode_alert_data(dataset)
    decoded_alert_data.to_zarr(zarr_uri, mode="w")

    return zarr_uri
