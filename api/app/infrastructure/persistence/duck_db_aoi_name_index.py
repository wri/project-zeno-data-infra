import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set

import duckdb
import pyarrow as pa
import pyarrow.parquet as parquet

from app.domain.models.aoi_match import AoiMatch
from app.domain.models.custom_aoi import CustomAoiName
from app.domain.repositories.aoi_name_index import AoiNameIndex


@dataclass(frozen=True)
class NameSource:
    """Where one AOI type's names come from: a parquet with `aoi_id` and
    `area_ha`, the extra columns to read, and SQL expressions over them for each
    AOI's name, display name, list of alternate names, importance when no
    importance is known (0 to 1) and whether to leave it out of search."""

    uri: str
    columns: tuple[str, ...]
    name: str
    display_name: str
    fallback_importance: str
    alternate_names: str = "[]"
    excluded: str = "false"


class DuckDbAoiNameIndex(AoiNameIndex):
    """Searches AOI names held in memory, ignoring case and accents.

    A name or alternate name matches when it equals the text, starts with it,
    or has, for every word of the text, a word starting with that word. A word
    found in no name is first corrected to the nearest known word, within one
    edit per four letters, and matches found that way count for less.

    Each match is scored from how well its name or an alternate name matches,
    whether its display name contains the place given after a comma, and its
    importance, so a well-known place can outrank a better spelled obscure one.
    Ties go to the larger area.

    Importance and extra alternate names come from optional parquets keyed by
    `aoi_type` and `aoi_id`."""

    def __init__(
        self,
        sources: Dict[str, NameSource],
        importance_uri: Optional[str] = None,
        alternate_names_uri: Optional[str] = None,
        word_separators: str = r"[\s,.;:()\[\]/'\"-]+",
        corrected_match_weight: float = 0.8,
        custom_aoi_importance: float = 1.0,
    ):
        self.word_separators = word_separators
        self.corrected_match_weight = corrected_match_weight
        self.custom_aoi_importance = custom_aoi_importance
        self.connection = duckdb.connect()
        self.connection.execute("CREATE SEQUENCE text_ids")
        self.connection.execute(
            "CREATE TABLE names (aoi_type VARCHAR, aoi_id VARCHAR, name VARCHAR,"
            " display_name VARCHAR, fallback_importance DOUBLE, area_ha DOUBLE,"
            " search_display_name VARCHAR)"
        )
        self.connection.execute(
            "CREATE TABLE search_texts (text_id BIGINT, aoi_type VARCHAR,"
            " aoi_id VARCHAR, is_alternate BOOLEAN, text VARCHAR)"
        )
        self.connection.execute(
            "CREATE TABLE importance (aoi_type VARCHAR, aoi_id VARCHAR,"
            " importance DOUBLE)"
        )
        for aoi_type, source in sources.items():
            self._add_source(aoi_type, source)
        if alternate_names_uri is not None:
            self._add_alternate_names(alternate_names_uri)
        if importance_uri is not None:
            self._add_importance(importance_uri)
        self._index_words()

    def _add_source(self, aoi_type: str, source: NameSource) -> None:
        raw = parquet.read_table(
            source.uri, columns=["aoi_id", "area_ha", *source.columns]
        )
        self.connection.register("raw", raw)
        self.connection.execute(
            f"""
            CREATE TEMP TABLE source AS
            SELECT aoi_id, area_ha, {source.name} AS name,
                {source.display_name} AS display_name,
                {source.fallback_importance} AS fallback_importance,
                {source.alternate_names} AS alternate_names
            FROM raw
            WHERE NOT coalesce({source.excluded}, false) AND name IS NOT NULL
            """
        )
        self.connection.execute(
            """
            INSERT INTO names
            SELECT ?, aoi_id, name, display_name, fallback_importance, area_ha,
                strip_accents(lower(display_name))
            FROM source
            """,
            [aoi_type],
        )
        self.connection.execute(
            """
            INSERT INTO search_texts
            SELECT nextval('text_ids'), ?, aoi_id, false,
                strip_accents(lower(trim(name)))
            FROM source
            """,
            [aoi_type],
        )
        self.connection.execute(
            "CREATE TEMP TABLE raw_alternate_names AS"
            " SELECT ? AS aoi_type, aoi_id, unnest(alternate_names) AS alternate_name"
            " FROM source",
            [aoi_type],
        )
        self._add_raw_alternate_names()
        self.connection.execute("DROP TABLE raw_alternate_names")
        self.connection.execute("DROP TABLE source")
        self.connection.unregister("raw")

    def _add_alternate_names(self, uri: str) -> None:
        self.connection.register("raw_alternate_names", parquet.read_table(uri))
        self._add_raw_alternate_names()
        self.connection.unregister("raw_alternate_names")

    def _add_raw_alternate_names(self) -> None:
        self.connection.execute(
            """
            INSERT INTO search_texts
            SELECT nextval('text_ids'), aoi_type, aoi_id, true, text
            FROM (
                SELECT DISTINCT aoi_type, aoi_id,
                    strip_accents(lower(trim(alternate_name))) AS text
                FROM raw_alternate_names
                WHERE nullif(trim(alternate_name), '') IS NOT NULL
            )
            """
        )

    def _add_importance(self, uri: str) -> None:
        self.connection.register("raw", parquet.read_table(uri))
        self.connection.execute(
            "INSERT INTO importance SELECT aoi_type, aoi_id, importance FROM raw"
        )
        self.connection.unregister("raw")

    def _index_words(self) -> None:
        self.connection.execute(
            """
            CREATE TABLE text_words AS
            SELECT DISTINCT text_id, word
            FROM (
                SELECT text_id,
                    unnest(regexp_split_to_array(text, $separators)) AS word
                FROM search_texts
            )
            WHERE word <> ''
            """,
            {"separators": self.word_separators},
        )
        self.connection.execute(
            "CREATE TABLE vocabulary AS"
            " SELECT word, count(*) AS texts FROM text_words GROUP BY word"
        )

    async def search(
        self,
        text: str,
        aoi_types: Optional[Sequence[str]],
        limit: int,
        custom_aois: Sequence[CustomAoiName] = (),
    ) -> List[AoiMatch]:
        return await asyncio.to_thread(
            self._search, text, aoi_types, limit, custom_aois
        )

    def _search(
        self,
        text: str,
        aoi_types: Optional[Sequence[str]],
        limit: int,
        custom_aois: Sequence[CustomAoiName],
    ) -> List[AoiMatch]:
        cursor = self.connection.cursor()
        place, _, containing_place = text.partition(",")
        words = self._words(cursor, place)
        if not words:
            return []
        custom_words = {
            word for aoi in custom_aois for word in self._words(cursor, aoi.name)
        }
        known_words = [
            self._known_or_nearest(cursor, word, custom_words) for word in words
        ]
        cursor.register(
            "custom_aois",
            pa.table(
                {
                    "aoi_id": [aoi.aoi_id for aoi in custom_aois],
                    "name": [aoi.name for aoi in custom_aois],
                },
                schema=pa.schema([("aoi_id", pa.string()), ("name", pa.string())]),
            ),
        )
        rows = cursor.execute(
            """
            WITH custom AS (
                SELECT -row_number() OVER () AS text_id, aoi_id, name,
                    strip_accents(lower(trim(name))) AS text
                FROM custom_aois
            ),
            all_names AS (
                SELECT aoi_type, aoi_id, name, display_name, fallback_importance,
                    area_ha, search_display_name
                FROM names
                UNION ALL
                SELECT 'custom_aoi', aoi_id, name, name, $custom_importance, NULL,
                    text
                FROM custom
            ),
            all_texts AS (
                SELECT text_id, aoi_type, aoi_id, is_alternate, text FROM search_texts
                UNION ALL
                SELECT text_id, 'custom_aoi', aoi_id, false, text FROM custom
            ),
            all_words AS (
                SELECT text_id, word FROM text_words
                UNION ALL
                SELECT text_id, word FROM (
                    SELECT text_id,
                        unnest(regexp_split_to_array(text, $separators)) AS word
                    FROM custom
                )
                WHERE word <> ''
            ),
            query AS (
                SELECT strip_accents(lower(trim($place))) AS place,
                    strip_accents(lower(trim($containing_place)))
                        AS containing_place
            ),
            query_words AS (
                SELECT unnest($words) AS word,
                    generate_subscripts($words, 1) AS position
            ),
            texts_with_every_word AS (
                SELECT all_words.text_id
                FROM all_words
                JOIN query_words ON starts_with(all_words.word, query_words.word)
                GROUP BY all_words.text_id
                HAVING count(DISTINCT query_words.position) = len($words)
            ),
            text_matches AS (
                SELECT aoi_type, aoi_id,
                    CASE
                        WHEN text = query.place
                            THEN CASE WHEN is_alternate THEN 0.9 ELSE 1.0 END
                        WHEN starts_with(text, query.place)
                            THEN CASE WHEN is_alternate THEN 0.7 ELSE 0.75 END
                        ELSE CASE WHEN is_alternate THEN 0.55 ELSE 0.6 END
                            * $word_match_weight
                    END AS match
                FROM all_texts, query
                WHERE (
                    text = query.place
                    OR starts_with(text, query.place)
                    OR text_id IN (SELECT text_id FROM texts_with_every_word)
                )
                AND ($aoi_types IS NULL OR list_contains($aoi_types, aoi_type))
            ),
            best_matches AS (
                SELECT aoi_type, aoi_id, max(match) AS match
                FROM text_matches
                GROUP BY aoi_type, aoi_id
            )
            SELECT names.aoi_type, names.aoi_id, names.name, names.display_name
            FROM best_matches
            JOIN all_names AS names USING (aoi_type, aoi_id)
            LEFT JOIN importance USING (aoi_type, aoi_id), query
            ORDER BY
                0.5 * best_matches.match
                + 0.2 * (query.containing_place <> ''
                    AND contains(names.search_display_name,
                        query.containing_place))::INTEGER
                + 0.3 * coalesce(importance.importance,
                    names.fallback_importance) DESC,
                names.area_ha DESC NULLS LAST
            LIMIT $limit
            """,
            {
                "place": place,
                "containing_place": containing_place,
                "words": known_words,
                "word_match_weight": (
                    1.0 if known_words == words else self.corrected_match_weight
                ),
                "aoi_types": None if aoi_types is None else list(aoi_types),
                "limit": limit,
                "custom_importance": self.custom_aoi_importance,
                "separators": self.word_separators,
            },
        ).fetchall()
        cursor.unregister("custom_aois")
        return [AoiMatch(*row) for row in rows]

    def _words(self, cursor, text: str) -> List[str]:
        """The text's words, normalized and split the same way as names."""
        return cursor.execute(
            "SELECT list_filter(regexp_split_to_array("
            "strip_accents(lower(trim($text))), $separators), word -> word <> '')",
            {"text": text, "separators": self.word_separators},
        ).fetchone()[0]

    def _known_or_nearest(self, cursor, word: str, custom_words: Set[str]) -> str:
        """The word itself if some name has a word starting with it, else the
        nearest word any name has, preferring the most common. A word with no
        near neighbour is kept, so it matches nothing."""
        if any(custom_word.startswith(word) for custom_word in custom_words):
            return word
        if cursor.execute(
            "SELECT 1 FROM vocabulary WHERE starts_with(word, $word) LIMIT 1",
            {"word": word},
        ).fetchone():
            return word
        nearest = cursor.execute(
            """
            SELECT word FROM vocabulary
            WHERE abs(length(word) - length($word)) <= $max_edits
                AND levenshtein(word, $word) <= $max_edits
            ORDER BY levenshtein(word, $word), texts DESC, word
            LIMIT 1
            """,
            {"word": word, "max_edits": max(1, len(word) // 4)},
        ).fetchone()
        return word if nearest is None else nearest[0]
