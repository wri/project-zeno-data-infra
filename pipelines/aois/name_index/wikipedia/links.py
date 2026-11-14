import csv
from dataclasses import dataclass
from typing import Callable, Mapping, NamedTuple, Sequence

import pandas as pd


def all_aois(aois: pd.DataFrame) -> pd.Series:
    return pd.Series(True, index=aois.index)


def unchanged(identifiers: pd.Series) -> pd.Series:
    return identifiers


def at_admin_level(level: int) -> Callable[[pd.DataFrame], pd.Series]:
    """Applies a link only to admin areas of one level."""

    def applies_to(aois: pd.DataFrame) -> pd.Series:
        return aois["adm_level"] == level

    return applies_to


def without_revision(identifiers: pd.Series) -> pd.Series:
    """GADM ids as Wikidata writes them, without the "_1" revision suffix."""
    return identifiers.str.replace(r"_\d+$", "", regex=True)


@dataclass(frozen=True)
class WikidataLink:
    """How one Wikidata identifier property links Wikidata items to AOIs of one
    type: which AOIs it applies to, the AOI attribute holding the identifier,
    and how to turn Wikidata's values into the same form."""

    wikidata_property: str
    aoi_type: str
    aoi_identifier: str = "aoi_id"
    applies_to: Callable[[pd.DataFrame], pd.Series] = all_aois
    wikidata_identifier: Callable[[pd.Series], pd.Series] = unchanged


class WikipediaDetails(NamedTuple):
    importance: pd.DataFrame
    alternate_names: pd.DataFrame


def link_to_wikidata(
    aois: Mapping[str, pd.DataFrame],
    identifiers: Mapping[str, pd.DataFrame],
    links: Sequence[WikidataLink],
) -> pd.DataFrame:
    """Every (aoi_type, aoi_id, wikidata_id) that some link connects."""
    linked = [
        linked_by(link, aois[link.aoi_type], identifiers[link.wikidata_property])
        for link in links
    ]
    return pd.concat(linked, ignore_index=True).drop_duplicates()


def linked_by(
    link: WikidataLink, aois: pd.DataFrame, identifiers: pd.DataFrame
) -> pd.DataFrame:
    applicable = aois[link.applies_to(aois)]
    aoi_identifiers = pd.DataFrame(
        {
            "aoi_id": applicable["aoi_id"],
            "identifier": applicable[link.aoi_identifier].astype("string"),
        }
    ).dropna(subset=["identifier"])
    wikidata_identifiers = pd.DataFrame(
        {
            "wikidata_id": identifiers["wikidata_id"],
            "identifier": link.wikidata_identifier(identifiers["id"]).astype("string"),
        }
    )
    joined = aoi_identifiers.merge(wikidata_identifiers, on="identifier")
    return joined.assign(aoi_type=link.aoi_type)[["aoi_type", "aoi_id", "wikidata_id"]]


def wikipedia_details(
    links: pd.DataFrame, wikimedia_path: str, chunk_rows: int = 1_000_000
) -> WikipediaDetails:
    """Each linked AOI's importance, from its best-known linked item across
    Wikipedia languages, and that item's article titles as alternate names.

    Titles of redirects are left out, since they include sub-topics such as
    "History of …" and places inside the AOI. Underscores and a trailing
    disambiguation such as " (state)" are removed."""
    articles = linked_articles(wikimedia_path, set(links["wikidata_id"]), chunk_rows)
    item_importance = articles.groupby("wikidata_id", as_index=False)[
        "importance"
    ].max()
    importance = (
        links.merge(item_importance, on="wikidata_id")
        .sort_values(["importance", "wikidata_id"], ascending=[False, True])
        .drop_duplicates(["aoi_type", "aoi_id"])
        .reset_index(drop=True)
    )
    titles = articles[articles["type"] == "a"]
    alternate_names = (
        importance.merge(titles, on="wikidata_id")
        .assign(alternate_name=lambda rows: cleaned_titles(rows["title"]))[
            ["aoi_type", "aoi_id", "alternate_name"]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return WikipediaDetails(importance, alternate_names)


def linked_articles(
    wikimedia_path: str, wikidata_ids: set[str], chunk_rows: int
) -> pd.DataFrame:
    """The rows of Nominatim's Wikimedia importance file for the given items,
    read in chunks since the whole file is large."""
    chunks = pd.read_csv(
        wikimedia_path,
        sep="\t",
        usecols=["type", "title", "importance", "wikidata_id"],
        dtype={"type": str, "title": str, "importance": float, "wikidata_id": str},
        keep_default_na=False,
        quoting=csv.QUOTE_NONE,
        chunksize=chunk_rows,
    )
    return pd.concat(
        [chunk[chunk["wikidata_id"].isin(wikidata_ids)] for chunk in chunks],
        ignore_index=True,
    )


def cleaned_titles(titles: pd.Series) -> pd.Series:
    return (
        titles.str.replace("_", " ")
        .str.replace(r" \([^)]*\)$", "", regex=True)
        .str.strip()
    )
