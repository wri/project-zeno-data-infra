import tempfile
from datetime import date, datetime, timezone
from functools import partial
from pathlib import Path

import fsspec
from pipelines.aois.aoi_source import AoiSource
from pipelines.aois.name_index.aoi_attributes import read_aoi_attributes
from pipelines.aois.name_index.links import link_to_wikidata, wikipedia_details
from pipelines.aois.name_index.metadata import (
    AoiFile,
    NameIndexMetadata,
    WikidataQuery,
    WikimediaImportanceFile,
    write_metadata,
)
from pipelines.aois.name_index.name_records import name_records
from pipelines.aois.name_index.search_index import write_search_index
from pipelines.aois.name_index.specs import name_specs, wikidata_links
from pipelines.aois.name_index.wikidata import WikidataIdentifiers
from pipelines.aois.name_index.wikimedia_importance import (
    WikimediaImportanceCache,
    download_wikimedia_importance,
)
from pipelines.aois.sources import aoi_prefix
from pipelines.globals import ANALYTICS_BUCKET
from pipelines.utils import s3_uri_exists
from prefect import flow, task


@flow(name="AOI name index")
def aoi_name_index_flow(
    sources: tuple[AoiSource, ...],
    build_date: date,
    overwrite: bool = False,
    refresh_wikimedia_importance: bool = False,
) -> str:
    """Writes the Tantivy index AOI search loads, built from each AOI source's
    GeoParquet and from Wikipedia, with a metadata file of what it was built
    from. The metadata is written last, so it marks a complete build.

    Wikipedia importance comes from the newest cached copy of Nominatim's file,
    unless a refresh is asked for."""
    version = f"v{build_date:%Y%m%d}"
    prefix = f"s3://{ANALYTICS_BUCKET}/vectors/aois/search/{version}"
    metadata_uri = f"{prefix}/metadata.json"
    if not overwrite and s3_uri_exists(metadata_uri):
        return prefix

    word_pattern = r"[^\s,.;:()\[\]/'\"-]+"
    user_agent = "gnw-aoi-search/0.1"
    wikidata = WikidataIdentifiers("https://query.wikidata.org/sparql", user_agent)
    wikimedia_url = "https://nominatim.org/data/wikimedia-importance.csv.gz"
    wikimedia_cache = WikimediaImportanceCache(
        wikimedia_url,
        f"s3://{ANALYTICS_BUCKET}/vectors/aois/search/source/wikimedia-importance",
        partial(download_wikimedia_importance, user_agent=user_agent),
    )
    specs = name_specs()
    links = wikidata_links()

    aoi_files = aoi_files_for(specs, sources)
    aois = {}
    for aoi_file in aoi_files:
        aois[aoi_file.aoi_type] = task(read_aoi_attributes)(aoi_file.uri)

    properties = sorted({link.wikidata_property for link in links})
    queried_at = datetime.now(timezone.utc)
    identifiers = {}
    for wikidata_property in properties:
        identifiers[wikidata_property] = task(wikidata.fetch)(wikidata_property)

    with tempfile.TemporaryDirectory() as directory:
        wikimedia_path = str(Path(directory) / "wikimedia-importance.csv.gz")
        wikimedia = task(wikimedia_cache.fetch)(
            wikimedia_path, refresh_wikimedia_importance
        )
        linked = task(link_to_wikidata)(aois, identifiers, links)
        importance, alternate_names = task(wikipedia_details)(linked, wikimedia_path)

    records = task(name_records)(aois, specs, importance, alternate_names)
    with tempfile.TemporaryDirectory() as directory:
        task(write_search_index)(records, directory, word_pattern)
        task(upload_directory)(directory, f"{prefix}/index")

    write_metadata(
        NameIndexMetadata(
            version=version,
            word_pattern=word_pattern,
            aoi_files=aoi_files,
            wikidata=WikidataQuery(
                endpoint=wikidata.endpoint,
                properties=properties,
                queried_at=queried_at.isoformat(),
            ),
            wikimedia_importance=WikimediaImportanceFile(
                wikimedia_url, wikimedia.source_date, wikimedia.cache_uri
            ),
        ),
        metadata_uri,
    )
    return prefix


def aoi_files_for(specs, sources: tuple[AoiSource, ...]) -> list[AoiFile]:
    """The GeoParquet each named AOI type is built from."""
    by_name = {source.source: source for source in sources}
    aoi_files = []
    for spec in specs:
        source = by_name[spec.source]
        aoi_files.append(
            AoiFile(
                aoi_type=spec.aoi_type,
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
