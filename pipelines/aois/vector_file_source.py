import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import boto3
import geopandas as gpd
import pandas as pd

from pipelines.aois.aoi_source import AoiSource
from pipelines.utils import parse_s3_uri


@dataclass(frozen=True)
class VectorFileSource(AoiSource):
    """A vector file on S3 whose AOI ids are one of its columns."""

    source: str
    version: str
    uri: str
    id_column: str
    layer: Optional[str] = None
    dropped_columns: tuple[str, ...] = ()

    def load(self) -> gpd.GeoDataFrame:
        """Downloads the file first, since GDAL's random reads over S3 are very
        slow for GeoPackages and FileGDBs."""
        bucket, key = parse_s3_uri(self.uri)
        with tempfile.TemporaryDirectory() as directory:
            local_path = Path(directory) / Path(key).name
            boto3.client("s3").download_file(
                bucket, key, str(local_path), ExtraArgs={"RequestPayer": "requester"}
            )
            return gpd.read_file(local_path, layer=self.layer)

    def aoi_ids(self, attributes: pd.DataFrame) -> pd.Series:
        values = attributes[self.id_column]
        if pd.api.types.is_float_dtype(values):
            # integer ids are read as floats when some are missing (101 -> 101.0)
            values = values.astype("Int64")
        return values.astype("string")
