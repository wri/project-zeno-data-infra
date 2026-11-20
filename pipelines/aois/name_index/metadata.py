import json
from dataclasses import asdict, dataclass
from typing import Any

import fsspec


@dataclass(frozen=True)
class AoiFile:
    aoi_type: str
    source: str
    version: str
    uri: str


@dataclass(frozen=True)
class NameIndexMetadata:
    """What one build of the AOI name index was made from, and the pattern its
    names were split into words with, which search must split queries with."""

    version: str
    word_pattern: str
    aoi_files: list[AoiFile]
    external_importance: dict[str, Any]


def write_metadata(metadata: NameIndexMetadata, uri: str) -> str:
    with fsspec.open(uri, "w") as file:
        json.dump(asdict(metadata), file, indent=2)
    return uri
