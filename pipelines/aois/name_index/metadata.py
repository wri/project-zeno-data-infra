import json
from dataclasses import asdict, dataclass

import fsspec


@dataclass(frozen=True)
class AoiFile:
    aoi_type: str
    source: str
    version: str
    uri: str


@dataclass(frozen=True)
class WikidataQuery:
    """Wikidata has no version, so the time of the last query stands in for
    one."""

    endpoint: str
    properties: list[str]
    queried_at: str


@dataclass(frozen=True)
class WikimediaImportanceFile:
    """The file has no version, so the date its source was last modified
    stands in for one."""

    url: str
    source_date: str
    cache_uri: str


@dataclass(frozen=True)
class NameIndexMetadata:
    """What one build of the AOI name index was made from, and how its texts
    were split into words, which search queries must match."""

    version: str
    word_separators: str
    aoi_files: list[AoiFile]
    wikidata: WikidataQuery
    wikimedia_importance: WikimediaImportanceFile


def write_metadata(metadata: NameIndexMetadata, uri: str) -> str:
    with fsspec.open(uri, "w") as file:
        json.dump(asdict(metadata), file, indent=2)
    return uri
