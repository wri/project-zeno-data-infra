import tempfile
from datetime import date
from functools import partial
from typing import Sequence

import fsspec
from prefect import flow, task

from pipelines.aois.aoi_source import AoiSource
from pipelines.aois.name_index.aoi_attributes import read_aoi_attributes
from pipelines.aois.name_index.external_importance import ExternalImportance
from pipelines.aois.name_index.metadata import (
    AoiFile,
    NameIndexMetadata,
    write_metadata,
)
from pipelines.aois.name_index.name_records import name_records
from pipelines.aois.name_index.namers.aoi_namer import AoiNamer
from pipelines.aois.name_index.namers.gadm_names import GadmNames
from pipelines.aois.name_index.namers.kba_names import KbaNames
from pipelines.aois.name_index.namers.landmark_names import LandmarkNames
from pipelines.aois.name_index.namers.wdpa_names import WdpaNames
from pipelines.aois.name_index.search_index import write_search_index
from pipelines.aois.name_index.wikipedia.wikidata import WikidataIdentifiers
from pipelines.aois.name_index.wikipedia.wikimedia_importance import (
    WikimediaImportanceCache,
    download_wikimedia_importance,
)
from pipelines.aois.name_index.wikipedia.wikipedia_importance import (
    WikipediaImportance,
    wikidata_links,
)
from pipelines.aois.sources import aoi_prefix
from pipelines.globals import ANALYTICS_BUCKET
from pipelines.utils import s3_uri_exists


@flow(name="AOI name index")
def aoi_name_index_flow(
    sources: tuple[AoiSource, ...],
    build_date: date,
    overwrite: bool = False,
    refresh_importance: bool = False,
) -> str:
    """Writes the Tantivy index AOI search loads, built from each AOI source's
    GeoParquet and an external source of importance, with a metadata file of
    what it was built from. The metadata is written last, so it marks a
    complete build. External sources that are slow to change are cached
    unless a refresh is asked for."""
    version = f"v{build_date:%Y%m%d}"
    prefix = f"s3://{ANALYTICS_BUCKET}/vectors/aois/search/{version}"
    metadata_uri = f"{prefix}/metadata.json"
    if not overwrite and s3_uri_exists(metadata_uri):
        return prefix

    word_pattern = r"[^\s,.;:()\[\]/'\"-]+"
    namers = (GadmNames(), KbaNames(), WdpaNames(), LandmarkNames())
    external_importance = wikipedia_importance(refresh_importance)

    aoi_files = aoi_files_for(namers, sources)
    aois = {}
    for aoi_file in aoi_files:
        aois[aoi_file.aoi_type] = task(read_aoi_attributes)(aoi_file.uri)
    external = task(external_importance.details)(aois)
    records = task(name_records)(aois, namers, external)

    with tempfile.TemporaryDirectory() as directory:
        task(write_search_index)(records, directory, word_pattern)
        task(upload_directory)(directory, f"{prefix}/index")

    write_metadata(
        NameIndexMetadata(
            version=version,
            word_pattern=word_pattern,
            aoi_files=aoi_files,
            external_importance=external.sources,
        ),
        metadata_uri,
    )
    return prefix


def wikipedia_importance(refresh: bool) -> ExternalImportance:
    user_agent = "gnw-aoi-search/0.1"
    return WikipediaImportance(
        WikidataIdentifiers("https://query.wikidata.org/sparql", user_agent),
        WikimediaImportanceCache(
            "https://nominatim.org/data/wikimedia-importance.csv.gz",
            f"s3://{ANALYTICS_BUCKET}/vectors/aois/search/source/wikimedia-importance",
            partial(download_wikimedia_importance, user_agent=user_agent),
        ),
        wikidata_links(),
        refresh,
    )


def aoi_files_for(
    namers: Sequence[AoiNamer], sources: tuple[AoiSource, ...]
) -> list[AoiFile]:
    """The GeoParquet each named AOI type is built from."""
    by_name = {source.source: source for source in sources}
    aoi_files = []
    for namer in namers:
        source = by_name[namer.source]
        aoi_files.append(
            AoiFile(
                aoi_type=namer.aoi_type,
                source=source.source,
                version=source.version,
                uri=f"{aoi_prefix(source)}/aois.parquet",
            )
        )
    return aoi_files


def upload_directory(directory: str, uri: str) -> str:
    filesystem, path = fsspec.core.url_to_fs(uri)
    filesystem.put(f"{directory}/", f"{path}/", recursive=True)
    return uri
