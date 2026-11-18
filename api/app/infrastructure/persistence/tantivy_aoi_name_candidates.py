import asyncio
import json
import tempfile
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Sequence

import fsspec
import tantivy
from tantivy import Occur, Query

from app.domain.models.aoi_match import AoiMatch
from app.domain.models.aoi_search import (
    AoiCandidate,
    AoiSearch,
    MatchKind,
    NameMatch,
)
from app.domain.models.custom_aoi import CustomAoiName
from app.domain.repositories.aoi_name_candidates import AoiNameCandidates


def name_analyzers(word_pattern: str) -> Dict[str, tantivy.TextAnalyzer]:
    """How names and search text are compared, ignoring case and accents:
    "words" splits text into the words `word_pattern` matches, and "whole"
    keeps it as a single term."""
    return {
        "words": folded(tantivy.Tokenizer.regex(word_pattern)),
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
class AnalyzedSearch:
    """A search's text as Tantivy terms."""

    place: str
    words: List[str]
    text: str
    containing_words: List[str]

    @classmethod
    def of(
        cls, search: AoiSearch, analyzers: Dict[str, tantivy.TextAnalyzer]
    ) -> "AnalyzedSearch":
        return cls(
            place="".join(analyzers["whole"].analyze(search.place)),
            words=analyzers["words"].analyze(search.place),
            text="".join(analyzers["whole"].analyze(search.text)),
            containing_words=analyzers["words"].analyze(search.containing_place),
        )


@dataclass(frozen=True)
class MatchQuery:
    match: NameMatch
    query: Query


class NameQueries:
    """Builds one searcher's query for each kind of match. A word starting no
    word of the searcher's AOIs is corrected to the words nearest it."""

    def __init__(self, searcher: tantivy.Searcher, schema: tantivy.Schema):
        self.searcher = searcher
        self.schema = schema

    def match_queries(self, search: AnalyzedSearch) -> List[MatchQuery]:
        name_words, name_corrected = self.every_word("name_words", search.words)
        alternate_words, alternate_corrected = self.every_word(
            "alternate_words", search.words
        )
        return [
            MatchQuery(
                NameMatch(MatchKind.EXACT_NAME), self.term("name", search.place)
            ),
            MatchQuery(
                NameMatch(MatchKind.EXACT_ALTERNATE),
                self.term("alternate_names", search.place),
            ),
            MatchQuery(
                NameMatch(MatchKind.NAME_PREFIX), self.prefix("name", search.place)
            ),
            MatchQuery(
                NameMatch(MatchKind.ALTERNATE_PREFIX),
                self.prefix("alternate_names", search.place),
            ),
            MatchQuery(NameMatch(MatchKind.NAME_WORDS, name_corrected), name_words),
            MatchQuery(
                NameMatch(MatchKind.ALTERNATE_WORDS, alternate_corrected),
                alternate_words,
            ),
        ]

    def within_containing_place(self, search: AnalyzedSearch) -> Optional[Query]:
        if not search.containing_words:
            return None
        return Query.boolean_query(
            [
                (
                    Occur.Should,
                    self.every_word("display_name", search.containing_words)[0],
                ),
                (Occur.Should, self.prefix("name", search.text)),
                (Occur.Should, self.prefix("alternate_names", search.text)),
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


class TantivyAoiNameCandidates(AoiNameCandidates):
    """Finds candidates in the AOI name index the AOI pipeline writes: a
    Tantivy index, and a metadata file giving the pattern its names were split
    into words with. A user's custom AOIs are indexed in memory for each
    search."""

    def __init__(self, uri: str):
        with fsspec.open(f"{uri}/metadata.json") as file:
            metadata = json.load(file)
        self.analyzers = name_analyzers(metadata["word_pattern"])
        self.index = self.registered(tantivy.Index.open(local_copy(f"{uri}/index")))
        self.searcher = self.index.searcher()

    async def candidates(
        self, search: AoiSearch, custom_aois: Sequence[CustomAoiName] = ()
    ) -> List[AoiCandidate]:
        return await asyncio.to_thread(self.candidates_now, search, custom_aois)

    def candidates_now(
        self, search: AoiSearch, custom_aois: Sequence[CustomAoiName]
    ) -> List[AoiCandidate]:
        analyzed = AnalyzedSearch.of(search, self.analyzers)
        if not analyzed.words:
            return []
        candidates = list(
            self.found(self.searcher, analyzed, search.aoi_types, search.limit)
        )
        if custom_aois and searches_custom_aois(search):
            custom_searcher = self.custom_aoi_searcher(custom_aois)
            candidates += self.found(custom_searcher, analyzed, None, search.limit)
        return candidates

    def found(
        self,
        searcher: tantivy.Searcher,
        search: AnalyzedSearch,
        aoi_types: Optional[Sequence[str]],
        limit: int,
    ) -> Iterator[AoiCandidate]:
        """The most important `limit` AOIs for each kind of match, among all
        AOIs and among those within the containing place."""
        queries = NameQueries(searcher, self.index.schema)
        within = queries.within_containing_place(search)
        for match_query in queries.match_queries(search):
            query = self.of_types(match_query.query, aoi_types)
            yield from self.most_important(searcher, query, match_query, False, limit)
            if within is not None:
                yield from self.most_important(
                    searcher, all_of([query, within]), match_query, True, limit
                )

    def most_important(
        self,
        searcher: tantivy.Searcher,
        query: Query,
        match_query: MatchQuery,
        within_containing_place: bool,
        limit: int,
    ) -> Iterator[AoiCandidate]:
        result = searcher.search(query, limit, count=False, order_by_field="importance")
        for _, address in result.hits:
            document = searcher.doc(address)
            yield AoiCandidate(
                aoi=AoiMatch(
                    document.get_first("aoi_type"),
                    document.get_first("aoi_id"),
                    document.get_first("name"),
                    document.get_first("display_name"),
                ),
                match=match_query.match,
                within_containing_place=within_containing_place,
                importance=document.get_first("importance"),
                area_ha=document.get_first("area_ha"),
            )

    def of_types(self, query: Query, aoi_types: Optional[Sequence[str]]) -> Query:
        if aoi_types is None:
            return query
        of_types = Query.term_set_query(self.index.schema, "aoi_type", list(aoi_types))
        return all_of([query, of_types])

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
                    importance=0.0,
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


def searches_custom_aois(search: AoiSearch) -> bool:
    return search.aoi_types is None or "custom_aoi" in search.aoi_types


def local_copy(uri: str) -> str:
    """Tantivy reads an index from local files, so a remote one is copied to
    a temporary directory first."""
    directory = tempfile.mkdtemp(prefix="aoi-name-index-")
    filesystem, path = fsspec.core.url_to_fs(uri)
    filesystem.get(f"{path.rstrip('/')}/", f"{directory}/", recursive=True)
    return directory
