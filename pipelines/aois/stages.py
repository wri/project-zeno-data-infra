import tempfile
from pathlib import Path
from typing import Optional

import boto3
import geopandas as gpd
import pandas as pd
import shapely
from pyproj import Geod
from shapely.geometry.base import BaseGeometry

from pipelines.aois.aoi_source import AoiSource
from pipelines.utils import parse_s3_uri

SQUARE_METERS_PER_HECTARE = 10_000


def gdal_path(path: str) -> str:
    """Zipped datasets (e.g. a zipped FileGDB) are read through GDAL's zip
    virtual file system."""
    return f"/vsizip/{path}" if path.endswith(".zip") else path


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
    """Add the AOI identity and search columns, keeping the source attributes
    (lowercased) alongside them. Source attributes that share a name with an
    AOI column are prefixed with the source name."""
    features = features.to_crs("EPSG:4326").rename(columns=str.lower)
    attributes = features.drop(columns="geometry")
    aoi_columns = pd.DataFrame(
        {
            "aoi_id": attributes[source.id_column].astype(str),
            "source": source.source,
            "subtype": source.subtype,
            "name": attributes[list(source.name_columns)].apply(join_name, axis=1),
            "iso3": attributes[source.iso3_column].apply(split_iso3),
        },
        index=features.index,
    )
    attributes = attributes.rename(
        columns={
            column: f"{source.source}_{column}"
            for column in attributes.columns.intersection(aoi_columns.columns)
        }
    )
    return gpd.GeoDataFrame(
        pd.concat([aoi_columns, attributes], axis=1),
        geometry=features.geometry,
        crs=features.crs,
    ).reset_index(drop=True)


def join_name(parts: pd.Series) -> str:
    return ", ".join(
        str(part) for part in parts if pd.notna(part) and str(part).strip()
    )


def split_iso3(value) -> Optional[list[str]]:
    if pd.isna(value):
        return None
    codes = [code.strip() for code in str(value).split(";") if code.strip()]
    return codes or None


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


def with_extent(features: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add [west, south, east, north] for framing a map. Geometries crossing
    the antimeridian get west > east, following the GeoJSON convention."""
    return features.assign(extent=features.geometry.apply(extent))


def extent(geometry: BaseGeometry) -> list[float]:
    west, south, east, north = geometry.bounds
    if east - west <= 180:
        return [west, south, east, north]
    eastern = shapely.clip_by_rect(geometry, 0, -90, 180, 90)
    western = shapely.clip_by_rect(geometry, -180, -90, 0, 90)
    if eastern.is_empty or western.is_empty:
        return [west, south, east, north]
    return [eastern.bounds[0], south, western.bounds[2], north]


def write_geoparquet(features: gpd.GeoDataFrame, uri: str) -> str:
    """Write features sorted by id, so reads filtered on id touch few row
    groups."""
    features.sort_values("aoi_id").to_parquet(
        uri, index=False, write_covering_bbox=True
    )
    return uri


def write_csv(dataframe: pd.DataFrame, uri: str) -> str:
    dataframe.to_csv(uri, index=False)
    return uri


def build_search_index(uris: list[str], columns: list[str]) -> pd.DataFrame:
    """Combine the non-geometry columns of several AOI GeoParquets."""
    return pd.concat(
        [pd.read_parquet(uri, columns=columns) for uri in uris], ignore_index=True
    )


def write_parquet(dataframe: pd.DataFrame, uri: str) -> str:
    dataframe.to_parquet(uri, index=False)
    return uri
