"""Disturbance-specific zarr helpers.

``decode_alert_data`` is the disturbance transform applied when creating
the DIST alerts zarr.  ``create_zarr`` is a convenience wrapper around
the generic ``common_stages.create_zarr`` with the disturbance-specific
URIs and transform pre-filled.
"""

import numpy as np
import xarray as xr

from pipelines.globals import ANALYTICS_BUCKET, DATA_LAKE_BUCKET
from pipelines.prefect_flows.common_stages import create_zarr as _create_zarr


def decode_alert_data(band_data) -> xr.Dataset:
    """Convert encoded alert date_conf data into separate confidence
    and alert date variables."""
    alert_date = (band_data % 10000).astype(np.uint16)
    alert_conf = (band_data // 10000).astype(np.uint8)
    alert_conf.name = "confidence"
    alert_date.name = "alert_date"
    return xr.merge((alert_conf, alert_date))


def create_zarr(version, overwrite=False) -> str:
    """Create a DIST alerts zarr, decoding alert data."""
    base_folder = f"umd_glad_dist_alerts/{version}/raster/epsg-4326"
    zarr_uri = (
        f"s3://{ANALYTICS_BUCKET}/zarr/dist-alerts/{version}/umd_glad_dist_alerts.zarr"
    )
    cog_uri = f"s3://{DATA_LAKE_BUCKET}/{base_folder}/cog/default.tif"
    return _create_zarr(
        cog_uri, zarr_uri, transform=decode_alert_data, overwrite=overwrite
    )


__all__ = ["create_zarr", "decode_alert_data"]
