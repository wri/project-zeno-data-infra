import asyncio
import json
import tempfile
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

import fsspec
import tantivy
from tantivy import Occur, Query

from app.domain.models.aoi_match import AoiMatch
from app.domain.models.custom_aoi import CustomAoiName
from app.domain.repositories.aoi_name_index import AoiNameIndex


def name_analyzers(word_pattern: str) -> Dict[str, tantivy.TextAnalyzer]:
    """How names and search text are compared, ignoring case and accents:
    "words" splits text into the words `word_pattern` matches, and "whole"
    keeps it as a single term."""
    word_tokenizer = tantivy.Tokenizer.regex(word_pattern)
    return {
        "words": folded(word_tokenizer),
        "whole": folded(tantivy.Tokenizer.raw()),
    }


def folded(tokenizer: tantivy.Tokenizer) -> tantivy.TextAnalyzer:
    return (
        tantivy.TextAnalyzerBuilder(tokenizer)
        .filter(tantivy.Filter.lowercase())
        .filter(tantivy.Filter.ascii_fold())
        .build()
    )


@dataclass(frozen=True)
class SearchText:
    """Search text split into the place being looked for and, after a comma,
    a place containing it."""

    place: str
    words: List[str]
    whole_text: str
    containing_words: List[str]

    @classmethod
    def parse(
        cls, text: str, analyzers: Dict[str, tantivy.TextAnalyzer]
    ) -> "SearchText":
        place, _, containing_place = text.partition(",")
        return cls(
            place=whole(analyzers, place),
            words=analyzers["words"].analyze(place),
            whole_text=whole(analyzers, text),
            containing_words=analyzers["words"].analyze(containing_place),
        )


def whole(analyzers: Dict[str, tantivy.TextAnalyzer], text: str) -> str:
    return "".join(analyzers["whole"].analyze(text.strip()))


@dataclass(frozen=True)
class MatchRule:
    """AOIs matching `query` match the search text this well, from 0 to 1."""

    match: float
    query: Query


class NameQueries:
    """Builds the queries for one searcher's AOIs. A name or alternate name
    matches when it equals the text, starts with it, or has, for every word of
    the text, a word starting with that word. A word starting no word of the
    searcher's AOIs is corrected to the words nearest it, within one edit per
    four letters, and matches found that way count for less."""

    def __init__(self, searcher: tantivy.Searcher, schema: tantivy.Schema):
        self.searcher = searcher
        self.schema = schema

    def match_rules(
        self, text: SearchText, corrected_match_weight: float
    ) -> List[MatchRule]:
        name_words, name_corrected = self.every_word("name_words", text.words)
        alternate_words, alternate_corrected = self.every_word(
            "alternate_words", text.words
        )
        return [
            MatchRule(1.0, self.term("name", text.place)),
            MatchRule(0.9, self.term("alternate_names", text.place)),
            MatchRule(0.75, self.prefix("name", text.place)),
            MatchRule(0.7, self.prefix("alternate_names", text.place)),
            MatchRule(
                0.6 * (corrected_match_weight if name_corrected else 1.0), name_words
            ),
            MatchRule(
                0.55 * (corrected_match_weight if alternate_corrected else 1.0),
                alternate_words,
            ),
        ]

    def within_containing_place(self, text: SearchText) -> Optional[Query]:
        """AOIs whose display name has every word given after a comma, or
        whose name matches the whole text, comma included."""
        if not text.containing_words:
            return None
        return Query.boolean_query(
            [
                (
                    Occur.Should,
                    self.every_word("display_name", text.containing_words)[0],
                ),
                (Occur.Should, self.prefix("name", text.whole_text)),
                (Occur.Should, self.prefix("alternate_names", text.whole_text)),
            ]
        )

    def every_word(self, field: str, words: List[str]) -> tuple[Query, bool]:
        """A query for AOIs with every word in `field`, and whether any word had
        to be corrected."""
        queries = []
        corrected = False
        for word in words:
            if self.starts_a_word(field, word):
                queries.append(self.prefix(field, word))
            else:
                queries.append(self.prefix(field, word, edits(word)))
                corrected = True
        return all_of(queries), corrected

    def starts_a_word(self, field: str, word: str) -> bool:
        return bool(self.searcher.terms_with_prefix(field, word, limit=1))

    def term(self, field: str, text: str) -> Query:
        return Query.term_query(self.schema, field, text)

    def prefix(self, field: str, text: str, distance: int = 0) -> Query:
        return Query.fuzzy_term_query(
            self.schema, field, text, distance=distance, prefix=True
        )


def edits(word: str) -> int:
    """One edit per four letters, at most the two Tantivy allows."""
    return min(2, max(1, len(word) // 4))


def all_of(queries: Sequence[Query]) -> Query:
    return Query.boolean_query([(Occur.Must, query) for query in queries])


@dataclass(frozen=True)
class RankedMatch:
    score: float
    area_ha: float
    match: AoiMatch


class Ranking:
    """Scores each AOI from how well its name matches, whether it lies within
    the place given after a comma, and its importance, so a well-known place
    can outrank a better spelled obscure one. Ties go to the larger area."""

    def __init__(
        self,
        match_weight: float = 0.5,
        containing_place_weight: float = 0.2,
        importance_weight: float = 0.3,
    ):
        self.match_weight = match_weight
        self.containing_place_weight = containing_place_weight
        self.importance_weight = importance_weight

    def best(
        self,
        searcher: tantivy.Searcher,
        rules: List[MatchRule],
        containing_place: Optional[Query],
        limit: int,
    ) -> List[RankedMatch]:
        """Within each rule, AOIs differ only by importance and containing
        place, so the most important `limit` of each, with and without the
        containing place, include the best overall."""
        best: Dict[tuple[str, str], RankedMatch] = {}
        for rule in rules:
            for bonus, query in self.variants(rule.query, containing_place):
                for importance, address in searcher.search(
                    query, limit, count=False, order_by_field="importance"
                ).hits:
                    ranked = self.ranked(searcher.doc(address), rule, bonus)
                    key = (ranked.match.aoi_type, ranked.match.aoi_id)
                    if key not in best or ranked.score > best[key].score:
                        best[key] = ranked
        return list(best.values())

    def variants(self, query: Query, containing_place: Optional[Query]):
        yield 0.0, query
        if containing_place is not None:
            yield self.containing_place_weight, all_of([query, containing_place])

    def ranked(
        self, document: tantivy.Document, rule: MatchRule, bonus: float
    ) -> RankedMatch:
        return RankedMatch(
            score=self.match_weight * rule.match
            + bonus
            + self.importance_weight * document.get_first("importance"),
            area_ha=document.get_first("area_ha"),
            match=AoiMatch(
                document.get_first("aoi_type"),
                document.get_first("aoi_id"),
                document.get_first("name"),
                document.get_first("display_name"),
            ),
        )


class TantivyAoiNameIndex(AoiNameIndex):
    """Searches the AOI name index the AOI pipeline writes: a Tantivy index,
    and a metadata file giving the pattern its names were split into words
    with. A user's custom AOIs are indexed in memory for each search."""

    def __init__(
        self,
        uri: str,
        corrected_match_weight: float = 0.8,
        custom_aoi_importance: float = 1.0,
        ranking: Ranking = Ranking(),
    ):
        with fsspec.open(f"{uri}/metadata.json") as file:
            metadata = json.load(file)
        self.analyzers = name_analyzers(metadata["word_pattern"])
        self.index = self.registered(tantivy.Index.open(local_copy(f"{uri}/index")))
        self.searcher = self.index.searcher()
        self.corrected_match_weight = corrected_match_weight
        self.custom_aoi_importance = custom_aoi_importance
        self.ranking = ranking

    async def search(
        self,
        text: str,
        aoi_types: Optional[Sequence[str]],
        limit: int,
        custom_aois: Sequence[CustomAoiName] = (),
    ) -> List[AoiMatch]:
        return await asyncio.to_thread(
            self.search_now, text, aoi_types, limit, custom_aois
        )

    def search_now(
        self,
        text: str,
        aoi_types: Optional[Sequence[str]],
        limit: int,
        custom_aois: Sequence[CustomAoiName],
    ) -> List[AoiMatch]:
        search_text = SearchText.parse(text, self.analyzers)
        if not search_text.words:
            return []
        ranked = self.ranked(self.searcher, search_text, aoi_types, limit)
        if custom_aois and (aoi_types is None or "custom_aoi" in aoi_types):
            custom_searcher = self.custom_aoi_searcher(custom_aois)
            ranked += self.ranked(custom_searcher, search_text, None, limit)
        ranked.sort(key=lambda match: (match.score, match.area_ha), reverse=True)
        return [match.match for match in ranked[:limit]]

    def ranked(
        self,
        searcher: tantivy.Searcher,
        text: SearchText,
        aoi_types: Optional[Sequence[str]],
        limit: int,
    ) -> List[RankedMatch]:
        queries = NameQueries(searcher, self.index.schema)
        rules = queries.match_rules(text, self.corrected_match_weight)
        if aoi_types is not None:
            of_types = Query.term_set_query(
                self.index.schema, "aoi_type", list(aoi_types)
            )
            rules = [
                MatchRule(rule.match, all_of([rule.query, of_types])) for rule in rules
            ]
        return self.ranking.best(
            searcher, rules, queries.within_containing_place(text), limit
        )

    def custom_aoi_searcher(
        self, custom_aois: Sequence[CustomAoiName]
    ) -> tantivy.Searcher:
        index = self.registered(tantivy.Index(self.index.schema))
        writer = index.writer(heap_size=15_000_000, num_threads=1)
        for custom_aoi in custom_aois:
            writer.add_document(
                tantivy.Document(
                    aoi_type="custom_aoi",
                    aoi_id=custom_aoi.aoi_id,
                    name=custom_aoi.name,
                    name_words=custom_aoi.name,
                    display_name=custom_aoi.name,
                    importance=self.custom_aoi_importance,
                    area_ha=0.0,
                )
            )
        writer.commit()
        index.reload()
        return index.searcher()

    def registered(self, index: tantivy.Index) -> tantivy.Index:
        for name, analyzer in self.analyzers.items():
            index.register_tokenizer(name, analyzer)
        return index


def local_copy(uri: str) -> str:
    """Tantivy reads an index from local files, so a remote one is copied to
    a temporary directory first."""
    directory = tempfile.mkdtemp(prefix="aoi-name-index-")
    filesystem, path = fsspec.core.url_to_fs(uri)
    filesystem.get(f"{path.rstrip('/')}/", f"{directory}/", recursive=True)
    return directory
