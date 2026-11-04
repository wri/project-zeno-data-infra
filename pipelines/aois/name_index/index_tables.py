from dataclasses import dataclass
from typing import Mapping, NamedTuple, Sequence

import duckdb
import pyarrow as pa


@dataclass(frozen=True)
class NameSpec:
    """How one AOI type is named, as SQL over the attributes of its pipeline
    source's AOIs: its name, display name, list of alternate names, importance
    when Wikipedia gives none (0 to 1), and whether to leave it out of search."""

    aoi_type: str
    source: str
    name: str
    display_name: str
    fallback_importance: str
    alternate_names: str = "[]"
    excluded: str = "false"


class NameIndexTables(NamedTuple):
    """The tables AOI search loads. Search texts and their words are lowercased
    and without accents, and words are split the same way as search queries."""

    names: pa.Table
    importance: pa.Table
    search_texts: pa.Table
    text_words: pa.Table


def name_index_tables(
    aois: Mapping[str, pa.Table],
    specs: Sequence[NameSpec],
    importance: pa.Table,
    alternate_names: pa.Table,
    word_separators: str,
) -> NameIndexTables:
    """Every named AOI, with its names, its alternate names from both its
    source and Wikipedia, and their words."""
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE names (aoi_type VARCHAR, aoi_id VARCHAR, name VARCHAR,"
        " display_name VARCHAR, fallback_importance DOUBLE, area_ha DOUBLE,"
        " search_display_name VARCHAR)"
    )
    connection.execute(
        "CREATE TABLE alternate_names (aoi_type VARCHAR, aoi_id VARCHAR,"
        " alternate_name VARCHAR)"
    )
    for spec in specs:
        add_names(connection, spec, aois[spec.aoi_type])
    connection.register("wikipedia_names", alternate_names)
    connection.execute("INSERT INTO alternate_names SELECT * FROM wikipedia_names")
    search_texts = connection.sql(
        """
        WITH own_names AS (
            SELECT aoi_type, aoi_id, strip_accents(lower(trim(name))) AS text
            FROM names
        ),
        alternates AS (
            SELECT DISTINCT aoi_type, aoi_id,
                strip_accents(lower(trim(alternate_name))) AS text
            FROM alternate_names
            SEMI JOIN names USING (aoi_type, aoi_id)
            WHERE nullif(trim(alternate_name), '') IS NOT NULL
        ),
        texts AS (
            SELECT aoi_type, aoi_id, false AS is_alternate, text FROM own_names
            UNION ALL
            SELECT aoi_type, aoi_id, true, text
            FROM alternates
            ANTI JOIN own_names USING (aoi_type, aoi_id, text)
        )
        SELECT row_number() OVER (ORDER BY aoi_type, aoi_id, is_alternate, text)
                AS text_id,
            aoi_type, aoi_id, is_alternate, text
        FROM texts
        """
    ).to_arrow_table()
    connection.register("search_texts", search_texts)
    text_words = connection.sql(
        """
        SELECT DISTINCT text_id, word
        FROM (
            SELECT text_id, unnest(regexp_split_to_array(text, $separators)) AS word
            FROM search_texts
        )
        WHERE word <> ''
        """,
        params={"separators": word_separators},
    ).to_arrow_table()
    connection.register("wikipedia_importance", importance)
    return NameIndexTables(
        names=connection.sql("SELECT * FROM names").to_arrow_table(),
        importance=connection.sql(
            """
            SELECT aoi_type, aoi_id, wikidata_id, importance
            FROM wikipedia_importance
            SEMI JOIN names USING (aoi_type, aoi_id)
            """
        ).to_arrow_table(),
        search_texts=search_texts,
        text_words=text_words,
    )


def add_names(
    connection: duckdb.DuckDBPyConnection, spec: NameSpec, aois: pa.Table
) -> None:
    connection.register("aois", aois)
    connection.execute(
        f"""
        CREATE TEMP TABLE source AS
        SELECT aoi_id, area_ha, {spec.name} AS name,
            {spec.display_name} AS display_name,
            {spec.fallback_importance} AS fallback_importance,
            {spec.alternate_names} AS alternate_names
        FROM aois
        WHERE NOT coalesce({spec.excluded}, false) AND name IS NOT NULL
        """
    )
    connection.execute(
        """
        INSERT INTO names
        SELECT $aoi_type, aoi_id, name, display_name, fallback_importance, area_ha,
            strip_accents(lower(display_name))
        FROM source
        """,
        {"aoi_type": spec.aoi_type},
    )
    connection.execute(
        """
        INSERT INTO alternate_names
        SELECT $aoi_type, aoi_id, unnest(alternate_names) FROM source
        """,
        {"aoi_type": spec.aoi_type},
    )
    connection.execute("DROP TABLE source")
    connection.unregister("aois")
