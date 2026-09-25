import subprocess
import tempfile
from pathlib import Path

import boto3
import geopandas as gpd
import pandas as pd
from pipelines.concessions.concession_source import ConcessionSource
from pipelines.utils import parse_s3_uri
from pyproj import Geod

SQUARE_METERS_PER_HECTARE = 10_000


def load_source(uri: str) -> gpd.GeoDataFrame:
    """Read a zipped vector dataset from a requester-pays S3 bucket."""
    bucket, key = parse_s3_uri(uri)
    with tempfile.TemporaryDirectory() as directory:
        local_path = Path(directory) / Path(key).name
        boto3.client("s3").download_file(
            bucket, key, str(local_path), ExtraArgs={"RequestPayer": "requester"}
        )
        return gpd.read_file(f"zip://{local_path}")


def normalize(features: gpd.GeoDataFrame, source: ConcessionSource) -> gpd.GeoDataFrame:
    """Assign row-number ids and concession columns, and harmonize attribute
    names. Ids follow source order so they line up with the GFW Data API."""
    features = features.to_crs("EPSG:4326").rename(columns=str.lower)
    attributes = features.drop(
        columns=["geometry", *source.dropped_columns], errors="ignore"
    )
    identifiers = pd.DataFrame(
        {
            "aoi_id": [str(i) for i in range(1, len(features) + 1)],
            "aoi_type": "concession",
            "concession_type": source.concession_type,
        },
        index=features.index,
    )
    return gpd.GeoDataFrame(
        pd.concat([identifiers, attributes], axis=1),
        geometry=features.geometry,
        crs=features.crs,
    ).reset_index(drop=True)


def with_area_ha(features: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    geod = Geod(ellps="WGS84")
    return features.assign(
        area_ha=features.geometry.apply(
            lambda geometry: abs(geod.geometry_area_perimeter(geometry)[0])
            / SQUARE_METERS_PER_HECTARE
        )
    )


def write_geoparquet(features: gpd.GeoDataFrame, uri: str) -> str:
    features.to_parquet(uri, index=False, write_covering_bbox=True)
    return uri


def build_pmtiles(features: gpd.GeoDataFrame, output_path: Path, layer: str) -> Path:
    """Tile features with tippecanoe. The GeoJSON input is written next to
    the output."""
    input_path = Path(output_path).with_suffix(".geojsonseq")
    features.to_file(input_path, driver="GeoJSONSeq")
    subprocess.run(
        [
            "tippecanoe",
            f"--output={output_path}",
            f"--layer={layer}",
            "--maximum-zoom=g",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            "--force",
            str(input_path),
        ],
        check=True,
    )
    return Path(output_path)


def upload_file(local_path: Path, uri: str) -> str:
    bucket, key = parse_s3_uri(uri)
    boto3.client("s3").upload_file(str(local_path), bucket, key)
    return uri


def write_csv(dataframe: pd.DataFrame, uri: str) -> str:
    dataframe.to_csv(uri, index=False)
    return uri
