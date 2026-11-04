import asyncio
import json
from typing import Dict, List, Optional, Sequence, Set

import duckdb
import fsspec
import pyarrow as pa
import pyarrow.parquet as parquet

from app.domain.models.aoi_match import AoiMatch
from app.domain.models.custom_aoi import CustomAoiName
from app.domain.repositories.aoi_name_index import AoiNameIndex


def index_columns() -> Dict[str, List[str]]:
    """The columns search reads from each of the index's tables."""
    return {
        "names": [
            "aoi_type",
            "aoi_id",
            "name",
            "display_name",
            "fallback_importance",
            "area_ha",
            "search_display_name",
        ],
        "importance": ["aoi_type", "aoi_id", "importance"],
        "search_texts": ["text_id", "aoi_type", "aoi_id", "is_alternate", "text"],
        "text_words": ["text_id", "word"],
    }


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

    Loads the index the AOI pipeline writes: a parquet per table, and a
    metadata file giving the separators its texts were split into words
    with."""

    def __init__(
        self,
        uri: str,
        corrected_match_weight: float = 0.8,
        custom_aoi_importance: float = 1.0,
    ):
        with fsspec.open(f"{uri}/metadata.json") as file:
            metadata = json.load(file)
        self.word_separators = metadata["word_separators"]
        self.connection = duckdb.connect()
        for table, columns in index_columns().items():
            self.connection.register(
                "raw", parquet.read_table(f"{uri}/{table}.parquet", columns=columns)
            )
            self.connection.execute(f"CREATE TABLE {table} AS SELECT * FROM raw")
            self.connection.unregister("raw")
        self.corrected_match_weight = corrected_match_weight
        self.custom_aoi_importance = custom_aoi_importance
        self.connection.execute(
            "CREATE TABLE vocabulary AS"
            " SELECT word, count(*) AS texts FROM text_words GROUP BY word"
        )
        self.connection.execute(
            """
            CREATE TABLE display_vocabulary AS
            SELECT word, count(*) AS texts
            FROM (
                SELECT unnest(regexp_split_to_array(search_display_name, $separators))
                    AS word
                FROM names
            )
            WHERE word <> ''
            GROUP BY word
            """,
            {"separators": self.word_separators},
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
        containing_words = [
            self._known_or_nearest(cursor, word, custom_words, "display_vocabulary")
            for word in self._words(cursor, containing_place)
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
                    strip_accents(lower(trim($text))) AS whole_text,
                    len($containing_words) > 0 AS has_containing_place
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
                    END AS match,
                    query.has_containing_place
                        AND starts_with(text, query.whole_text) AS matches_whole_query
                FROM all_texts, query
                WHERE (
                    text = query.place
                    OR starts_with(text, query.place)
                    OR text_id IN (SELECT text_id FROM texts_with_every_word)
                )
                AND ($aoi_types IS NULL OR list_contains($aoi_types, aoi_type))
            ),
            best_matches AS (
                SELECT aoi_type, aoi_id, max(match) AS match,
                    bool_or(matches_whole_query) AS matches_whole_query
                FROM text_matches
                GROUP BY aoi_type, aoi_id
            )
            SELECT names.aoi_type, names.aoi_id, names.name, names.display_name
            FROM best_matches
            JOIN all_names AS names USING (aoi_type, aoi_id)
            LEFT JOIN importance USING (aoi_type, aoi_id), query
            ORDER BY
                0.5 * best_matches.match
                + 0.2 * (best_matches.matches_whole_query OR (
                    query.has_containing_place AND NOT EXISTS (
                        SELECT 1 FROM unnest($containing_words) AS words(word)
                        WHERE NOT contains(names.search_display_name, words.word)
                    )))::INTEGER
                + 0.3 * coalesce(importance.importance,
                    names.fallback_importance) DESC,
                names.area_ha DESC NULLS LAST
            LIMIT $limit
            """,
            {
                "place": place,
                "text": text,
                "containing_words": containing_words,
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

    def _known_or_nearest(
        self,
        cursor,
        word: str,
        custom_words: Set[str],
        vocabulary: str = "vocabulary",
    ) -> str:
        """The word itself if some word in the vocabulary starts with it, else
        the nearest word in it, preferring the most common. A word with no near
        neighbour is kept, so it matches nothing. Names are corrected against
        the words of names, and places after a comma against the words of
        display names."""
        if any(custom_word.startswith(word) for custom_word in custom_words):
            return word
        if cursor.execute(
            f"SELECT 1 FROM {vocabulary} WHERE starts_with(word, $word) LIMIT 1",
            {"word": word},
        ).fetchone():
            return word
        nearest = cursor.execute(
            f"""
            SELECT word FROM {vocabulary}
            WHERE abs(length(word) - length($word)) <= $max_edits
                AND levenshtein(word, $word) <= $max_edits
            ORDER BY levenshtein(word, $word), texts DESC, word
            LIMIT 1
            """,
            {"word": word, "max_edits": max(1, len(word) // 4)},
        ).fetchone()
        return word if nearest is None else nearest[0]
