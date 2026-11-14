import gzip

import pandas as pd

from pipelines.aois.name_index.wikipedia.links import WikidataLink
from pipelines.aois.name_index.wikipedia.wikimedia_importance import (
    CachedWikimediaImportance,
)
from pipelines.aois.name_index.wikipedia.wikipedia_importance import (
    WikipediaImportance,
)


class FakeWikidata:
    endpoint = "https://wikidata.example/sparql"

    def fetch(self, wikidata_property):
        return pd.DataFrame({"wikidata_id": ["Q975"], "id": ["975"]})


class FakeWikimediaCache:
    url = "https://wikimedia.example/importance.csv.gz"

    def __init__(self):
        self.refreshes = []

    def fetch(self, destination, refresh):
        self.refreshes.append(refresh)
        with gzip.open(destination, "wt") as file:
            file.write(
                "language\ttype\ttitle\timportance\twikidata_id\n"
                "fr\ta\tParc_national_de_Yosemite\t0.59\tQ975\n"
            )
        return CachedWikimediaImportance("s3://cache/20251127/file", "20251127")


def test_linked_aois_get_wikipedia_importance_names_and_sources():
    cache = FakeWikimediaCache()
    wikipedia = WikipediaImportance(
        FakeWikidata(),
        cache,
        [WikidataLink("P809", "protected_area", "site_id")],
        refresh=True,
    )
    parks = pd.DataFrame({"aoi_id": ["975", "1"], "site_id": ["975", "1"]})

    details = wikipedia.details({"protected_area": parks})

    assert details.importance[["aoi_id", "importance"]].values.tolist() == [
        ["975", 0.59]
    ]
    assert details.alternate_names["alternate_name"].tolist() == [
        "Parc national de Yosemite"
    ]
    assert details.sources["wikimedia_importance"]["source_date"] == "20251127"
    assert details.sources["wikidata"]["properties"] == ["P809"]
    assert cache.refreshes == [True]
