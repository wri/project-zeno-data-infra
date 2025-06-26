import numpy as np
import pandas as pd
import xarray as xr

from .check_for_new_alerts import s3_object_exists


data_lake_bucket = "gfw-data-lake"


def decode_alert_data(band_data) -> xr.Dataset:
    """Convert encoded alert date_conf data into separate confidence and alert date variables."""
    alert_date = (band_data % 10000).astype(np.uint16)
    alert_conf = (band_data // 10000).astype(np.uint8)
    alert_conf.name = "confidence"
    alert_date.name = "alert_date"
    return xr.merge((alert_conf, alert_date))


def create_zarr(version, tile_uris, overwrite=False) -> str:
    """create a full extent zarr file in s3."""
    key = f"umd_glad_dist_alerts/{version}/raster/epsg-4326/zarr/umd_glad_dist_alerts_{version}.zarr"
    zarr_uri = f"s3://{data_lake_bucket}/{key}"

    if s3_object_exists(data_lake_bucket, f"{key}/zarr.json") and not overwrite:
        return zarr_uri

    dataset = xr.open_dataset(
        tile_uris,
        parallel=True,
        chunks={"x": 10000, "y": 10000},
    ).band_data

    decoded_alert_data = decode_alert_data(dataset)
    decoded_alert_data.to_zarr(zarr_uri, mode="w")

    return zarr_uri


def get_tiles(dataset, version):
    tiles_uri = f"s3://{data_lake_bucket}/{dataset}/{version}/raster/epsg-4326/10/40000/default/gdal-geotiff/tiles.geojson"
    tiles = pd.read_json(tiles_uri)

    tile_uris = tiles.features.apply(
        lambda x: "/".join(["s3:/"] + x["properties"]["name"].split("/")[2:])
    )

    return tile_uris
