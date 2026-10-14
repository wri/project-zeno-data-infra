import tempfile
from pathlib import Path
from typing import Callable, Optional

import boto3
import geopandas as gpd
import pandas as pd
from pyproj import Geod

from pipelines.aois.aoi_source import AoiSource
from pipelines.utils import parse_s3_uri

SQUARE_METERS_PER_HECTARE = 10_000


def gdal_path(path: str) -> str:
    """Zipped datasets (e.g. a zipped FileGDB) are read through GDAL's zip
    virtual file system."""
    return f"/vsizip/{path}" if path.endswith(".zip") else path


def vector_file(
    uri: str, layer: Optional[str] = None
) -> Callable[[], gpd.GeoDataFrame]:
    """Loads one layer of a vector dataset in a requester-pays S3 bucket."""
    return lambda: load_features(uri, layer)


def load_features(uri: str, layer: Optional[str]) -> gpd.GeoDataFrame:
    """Read one layer of a vector dataset from a requester-pays S3 bucket. The
    file is downloaded first, since GDAL's random reads over S3 are very slow
    for GeoPackages and FileGDBs."""
    bucket, key = parse_s3_uri(uri)
    with tempfile.TemporaryDirectory() as directory:
        local_path = Path(directory) / Path(key).name
        boto3.client("s3").download_file(
            bucket, key, str(local_path), ExtraArgs={"RequestPayer": "requester"}
        )
        return gpd.read_file(gdal_path(str(local_path)), layer=layer)


def normalize(features: gpd.GeoDataFrame, source: AoiSource) -> gpd.GeoDataFrame:
    """Add each row's AOI id, keeping the source attributes (lowercased)
    alongside it. Rows without an id can't be requested, so are dropped."""
    features = features.to_crs("EPSG:4326").rename(columns=str.lower)
    attributes = features.drop(columns="geometry")
    identified = gpd.GeoDataFrame(
        attributes.assign(aoi_id=source.aoi_ids(attributes)),
        geometry=features.geometry,
        crs=features.crs,
    )
    return identified[identified.aoi_id.notna()].reset_index(drop=True)


def with_area_ha(features: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    geod = Geod(ellps="WGS84")
    return features.assign(
        area_ha=features.geometry.apply(
            lambda geometry: abs(geod.geometry_area_perimeter(geometry)[0])
            / SQUARE_METERS_PER_HECTARE
        )
    )


def deduplicate(features: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Keep the largest feature for each id."""
    return (
        features.sort_values("area_ha", ascending=False, kind="stable")
        .drop_duplicates("aoi_id")
        .sort_index()
    )


def write_geoparquet(
    features: gpd.GeoDataFrame, uri: str, row_group_size: int = 500
) -> str:
    """Write features sorted by id in small row groups, so a read filtered on a
    few ids downloads only the row groups whose id range can hold them."""
    features.sort_values("aoi_id").to_parquet(
        uri, index=False, write_covering_bbox=True, row_group_size=row_group_size
    )
    return uri


def write_csv(dataframe: pd.DataFrame, uri: str) -> str:
    dataframe.to_csv(uri, index=False)
    return uri
