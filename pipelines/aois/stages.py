import tempfile
from pathlib import Path
from typing import Callable, Optional

import boto3
import geopandas as gpd
import pandas as pd
from pyproj import Geod

from pipelines.aois.aoi_source import AoiSource
from pipelines.utils import parse_s3_uri


def vector_file(
    uri: str, layer: Optional[str] = None
) -> Callable[[], gpd.GeoDataFrame]:
    return lambda: load_features(uri, layer)


def load_features(uri: str, layer: Optional[str]) -> gpd.GeoDataFrame:
    """Downloads the file first, since GDAL's random reads over S3 are very slow
    for GeoPackages and FileGDBs."""
    bucket, key = parse_s3_uri(uri)
    with tempfile.TemporaryDirectory() as directory:
        local_path = Path(directory) / Path(key).name
        boto3.client("s3").download_file(
            bucket, key, str(local_path), ExtraArgs={"RequestPayer": "requester"}
        )
        return gpd.read_file(local_path, layer=layer)


def normalize(features: gpd.GeoDataFrame, source: AoiSource) -> gpd.GeoDataFrame:
    features = features.to_crs("EPSG:4326").rename(columns=str.lower)
    return features.assign(aoi_id=source.aoi_ids(features)).reset_index(drop=True)


def drop_missing_ids(features: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return features[features.aoi_id.notna()]


def removed(
    before: gpd.GeoDataFrame, after: gpd.GeoDataFrame, reason: str
) -> pd.DataFrame:
    dropped = before.loc[before.index.difference(after.index)]
    return pd.DataFrame(dropped.drop(columns="geometry")).assign(reason=reason)


def with_area_ha(features: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    geod = Geod(ellps="WGS84")
    return features.assign(
        area_ha=features.geometry.apply(
            lambda geometry: abs(geod.geometry_area_perimeter(geometry)[0]) / 10_000
        )
    )


def write_geoparquet(
    features: gpd.GeoDataFrame, uri: str, row_group_size: int = 500
) -> str:
    """Sorted by id in small row groups, so reading a few ids only downloads the
    row groups whose id range can contain them."""
    features.sort_values("aoi_id").to_parquet(
        uri, index=False, write_covering_bbox=True, row_group_size=row_group_size
    )
    return uri


def write_csv(dataframe: pd.DataFrame, uri: str) -> str:
    dataframe.to_csv(uri, index=False)
    return uri
