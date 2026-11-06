import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Protocol, Sequence

import pandas as pd

from pipelines.aois.name_index.external_importance import (
    ExternalDetails,
    ExternalImportance,
)
from pipelines.aois.name_index.wikipedia.links import (
    WikidataLink,
    at_admin_level,
    link_to_wikidata,
    wikipedia_details,
    without_revision,
)
from pipelines.aois.name_index.wikipedia.wikimedia_importance import (
    CachedWikimediaImportance,
)


class WikidataSource(Protocol):
    endpoint: str

    def fetch(self, wikidata_property: str) -> pd.DataFrame: ...


class WikimediaImportanceSource(Protocol):
    url: str

    def fetch(self, destination: str, refresh: bool) -> CachedWikimediaImportance: ...


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
class WikipediaSources:
    wikidata: WikidataQuery
    wikimedia_importance: WikimediaImportanceFile


def wikidata_links() -> tuple[WikidataLink, ...]:
    """Wikidata identifier properties that match our AOI ids or attributes:
    GADM ids, ISO 3166-1 alpha-3 and ISO 3166-2 codes, HASC codes and WDPA
    ids."""
    return (
        WikidataLink("P8714", "admin", wikidata_identifier=without_revision),
        WikidataLink("P298", "admin", applies_to=at_admin_level(0)),
        WikidataLink("P300", "admin", "iso_1", at_admin_level(1)),
        WikidataLink("P8119", "admin", "hasc_1", at_admin_level(1)),
        WikidataLink("P8119", "admin", "hasc_2", at_admin_level(2)),
        WikidataLink("P809", "protected_area", "site_id"),
    )


class WikipediaImportance(ExternalImportance):
    """How well known AOIs are from Wikipedia page views, through Nominatim's
    Wikimedia importance file, for AOIs linked to Wikidata items by the given
    identifier properties. Their Wikipedia article titles become alternate
    names, which adds names in other languages.

    The Wikimedia file changes slowly, so its newest cached copy is used unless
    a refresh is asked for."""

    def __init__(
        self,
        wikidata: WikidataSource,
        wikimedia_importance: WikimediaImportanceSource,
        links: Sequence[WikidataLink],
        refresh: bool,
    ):
        self.wikidata = wikidata
        self.wikimedia_importance = wikimedia_importance
        self.links = links
        self.refresh = refresh

    def details(self, aois: Mapping[str, pd.DataFrame]) -> ExternalDetails:
        properties = sorted({link.wikidata_property for link in self.links})
        queried_at = datetime.now(timezone.utc)
        identifiers = {}
        for wikidata_property in properties:
            identifiers[wikidata_property] = self.wikidata.fetch(wikidata_property)
        linked = link_to_wikidata(aois, identifiers, self.links)

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "wikimedia-importance.csv.gz")
            cached = self.wikimedia_importance.fetch(path, self.refresh)
            importance, alternate_names = wikipedia_details(linked, path)

        sources = WikipediaSources(
            WikidataQuery(self.wikidata.endpoint, properties, queried_at.isoformat()),
            WikimediaImportanceFile(
                self.wikimedia_importance.url, cached.source_date, cached.cache_uri
            ),
        )
        return ExternalDetails(importance, alternate_names, asdict(sources))
