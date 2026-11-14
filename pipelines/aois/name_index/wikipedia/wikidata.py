import io
import urllib.parse
import urllib.request

import pandas as pd


def parse_identifiers(response: bytes) -> pd.DataFrame:
    """Wikidata ids and identifier values from a SPARQL CSV of `?item ?id`.
    Values are kept as text, so identifiers such as "NA" aren't read as
    missing."""
    rows = pd.read_csv(io.BytesIO(response), dtype=str, keep_default_na=False)
    return pd.DataFrame(
        {
            "wikidata_id": rows["item"].str.replace(r"^.*/", "", regex=True),
            "id": rows["id"],
        }
    )


class WikidataIdentifiers:
    """Fetches every value of a Wikidata identifier property, with one SPARQL
    query each."""

    def __init__(self, endpoint: str, user_agent: str):
        self.endpoint = endpoint
        self.user_agent = user_agent

    def fetch(self, wikidata_property: str) -> pd.DataFrame:
        query = f"SELECT ?item ?id WHERE {{ ?item wdt:{wikidata_property} ?id }}"
        request = urllib.request.Request(
            f"{self.endpoint}?{urllib.parse.urlencode({'query': query})}",
            headers={"Accept": "text/csv", "User-Agent": self.user_agent},
        )
        with urllib.request.urlopen(request, timeout=300) as response:
            return parse_identifiers(response.read())
