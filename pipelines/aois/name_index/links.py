from dataclasses import dataclass
from typing import Mapping, NamedTuple, Sequence

import duckdb
import pyarrow as pa


@dataclass(frozen=True)
class WikidataLink:
    """How one Wikidata identifier property links Wikidata items to AOIs of one
    type. SQL over the AOI's attributes gives the identifier to match and which
    AOIs the property applies to; SQL over Wikidata's value (`id`) turns it into
    the same form."""

    wikidata_property: str
    aoi_type: str
    aoi_identifier: str = "aoi_id"
    applies_to: str = "true"
    wikidata_identifier: str = "id"


class WikipediaDetails(NamedTuple):
    importance: pa.Table
    alternate_names: pa.Table


def link_to_wikidata(
    aois: Mapping[str, pa.Table],
    identifiers: Mapping[str, pa.Table],
    links: Sequence[WikidataLink],
) -> pa.Table:
    """Every (aoi_type, aoi_id, wikidata_id) that some link connects."""
    connection = duckdb.connect()
    selects = []
    for number, link in enumerate(links):
        connection.register(f"aois_{number}", aois[link.aoi_type])
        connection.register(
            f"identifiers_{number}", identifiers[link.wikidata_property]
        )
        selects.append(
            f"""
            SELECT '{link.aoi_type}' AS aoi_type, aois.aoi_id, wikidata.wikidata_id
            FROM (
                SELECT aoi_id,
                    CAST({link.aoi_identifier} AS VARCHAR) AS identifier
                FROM aois_{number}
                WHERE {link.applies_to}
            ) AS aois
            JOIN (
                SELECT wikidata_id, {link.wikidata_identifier} AS identifier
                FROM identifiers_{number}
            ) AS wikidata USING (identifier)
            """
        )
    return connection.sql(" UNION ".join(selects)).to_arrow_table()


def wikipedia_details(links: pa.Table, wikimedia_path: str) -> WikipediaDetails:
    """Each linked AOI's importance, from its best-known linked item across
    Wikipedia languages, and that item's article titles as alternate names.

    Titles of redirects are left out, since they include sub-topics such as
    "History of …" and places inside the AOI. Underscores and a trailing
    disambiguation such as " (state)" are removed."""
    connection = duckdb.connect()
    connection.register("links", links)
    connection.execute(
        """
        CREATE TABLE wikimedia AS
        SELECT * FROM read_csv($path, delim='\t', header=true, columns={
            'language': 'VARCHAR', 'type': 'VARCHAR', 'title': 'VARCHAR',
            'importance': 'DOUBLE', 'wikidata_id': 'VARCHAR'
        })
        WHERE wikidata_id IS NOT NULL
        """,
        {"path": wikimedia_path},
    )
    importance = connection.sql(
        """
        SELECT links.aoi_type, links.aoi_id, links.wikidata_id, items.importance
        FROM links
        JOIN (
            SELECT wikidata_id, max(importance) AS importance
            FROM wikimedia GROUP BY wikidata_id
        ) AS items USING (wikidata_id)
        QUALIFY row_number() OVER (
            PARTITION BY links.aoi_type, links.aoi_id
            ORDER BY items.importance DESC, links.wikidata_id
        ) = 1
        """
    ).to_arrow_table()
    connection.register("importance", importance)
    alternate_names = connection.sql(
        r"""
        SELECT DISTINCT importance.aoi_type, importance.aoi_id,
            trim(regexp_replace(replace(wikimedia.title, '_', ' '),
                ' \([^)]*\)$', '')) AS alternate_name
        FROM importance
        JOIN wikimedia USING (wikidata_id)
        WHERE wikimedia.type = 'a'
        """
    ).to_arrow_table()
    return WikipediaDetails(importance, alternate_names)
