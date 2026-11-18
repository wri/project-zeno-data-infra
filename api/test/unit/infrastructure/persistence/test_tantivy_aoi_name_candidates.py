import json

import pytest
import tantivy

from app.domain.models.custom_aoi import CustomAoiName
from app.domain.services.aoi_name_ranking import AoiNameRanking
from app.infrastructure.persistence.tantivy_aoi_name_candidates import (
    TantivyAoiNameCandidates,
    name_analyzers,
)
from app.use_cases.aois.aoi_name_search import AoiNameSearch

WORD_PATTERN = r"[^\s,.;:()\[\]/'\"-]+"


def schema():
    builder = tantivy.SchemaBuilder()
    builder.add_text_field("aoi_type", stored=True, tokenizer_name="raw")
    builder.add_text_field("aoi_id", stored=True, tokenizer_name="raw")
    builder.add_text_field("name", stored=True, tokenizer_name="whole")
    builder.add_text_field("name_words", tokenizer_name="words")
    builder.add_text_field("display_name", stored=True, tokenizer_name="words")
    builder.add_text_field("alternate_names", tokenizer_name="whole")
    builder.add_text_field("alternate_words", tokenizer_name="words")
    builder.add_float_field("importance", stored=True, fast=True)
    builder.add_float_field("area_ha", stored=True, fast=True)
    return builder.build()


def aoi(aoi_type, aoi_id, name, display_name, importance, area_ha, alternates=()):
    return tantivy.Document(
        aoi_type=aoi_type,
        aoi_id=aoi_id,
        name=name,
        name_words=name,
        display_name=display_name,
        alternate_names=list(alternates),
        alternate_words=list(alternates),
        importance=importance,
        area_ha=area_ha,
    )


def write_index(directory, documents):
    """Writes the files the AOI pipeline produces."""
    (directory / "index").mkdir(parents=True)
    index = tantivy.Index(schema(), path=str(directory / "index"))
    for name, analyzer in name_analyzers(WORD_PATTERN).items():
        index.register_tokenizer(name, analyzer)
    writer = index.writer(heap_size=15_000_000, num_threads=1)
    for document in documents:
        writer.add_document(document)
    writer.commit()
    writer.wait_merging_threads()
    (directory / "metadata.json").write_text(json.dumps({"word_pattern": WORD_PATTERN}))


@pytest.fixture
def index(tmp_path):
    write_index(
        tmp_path,
        [
            aoi("admin", "PRY", "Paraguay", "Paraguay", 0.77, 4.0e7),
            aoi(
                "admin",
                "BRA.14",
                "Pará",
                "Pará, Brazil",
                0.65,
                1.2e8,
                ["Grão-Pará"],
            ),
            aoi("admin", "BRA.18", "Paraná", "Paraná, Brazil", 0.68, 2.0e7),
            aoi("admin", "SUR.6", "Para", "Para, Suriname", 0.44, 1.5e5),
            aoi(
                "admin",
                "USA.5.22",
                "Yosemite West",
                "Yosemite West, California, United States",
                0.35,
                1.0e4,
            ),
            aoi(
                "admin",
                "USA.48",
                "Washington",
                "Washington, United States",
                0.76,
                1.8e7,
            ),
            aoi(
                "admin",
                "USA.9.1",
                "District of Columbia",
                "District of Columbia, District of Columbia, United States",
                0.81,
                1.8e4,
                ["Washington", "Washington, DC"],
            ),
            aoi("protected_area", "1", "Pari", "Pari, BRA", 0.15, 10.0),
            aoi(
                "protected_area",
                "975",
                "Yosemite National Park",
                "Yosemite National Park, USA",
                0.59,
                3.0e5,
                ["Parc national de Yosemite"],
            ),
            aoi(
                "protected_area",
                "4",
                "Parque Nacional da Amazônia",
                "Parque Nacional da Amazônia, BRA",
                0.15,
                1.0e6,
                ["Amazonia National Park"],
            ),
            aoi(
                "protected_area",
                "902780",
                "Oulanka National Park",
                "Oulanka National Park, FIN",
                0.15,
                2.7e4,
            ),
        ],
    )
    return AoiNameSearch(TantivyAoiNameCandidates(str(tmp_path)), AoiNameRanking())


async def ids(index, text, aoi_types=None):
    matches = await index.search(text, aoi_types=aoi_types, limit=10)
    return [match.aoi_id for match in matches]


@pytest.mark.asyncio
async def test_equally_good_matches_rank_by_importance(index):
    # Yosemite matches through its French name, "Parc national de Yosemite"
    assert await ids(index, "par") == [
        "PRY",
        "BRA.18",
        "BRA.14",
        "975",
        "SUR.6",
        "4",
        "1",
        "902780",
    ]


@pytest.mark.asyncio
async def test_a_famous_park_beats_an_obscure_admin_area_with_a_similar_name(index):
    assert await ids(index, "yosemite") == ["975", "USA.5.22"]


@pytest.mark.asyncio
async def test_text_after_a_comma_prefers_places_within_it(index):
    assert (await ids(index, "pará, brazil"))[:3] == ["BRA.14", "BRA.18", "SUR.6"]


@pytest.mark.asyncio
async def test_a_misspelled_place_after_a_comma_still_prefers_places_within_it(index):
    assert (await ids(index, "pará, brazl"))[:3] == ["BRA.14", "BRA.18", "SUR.6"]


@pytest.mark.asyncio
async def test_a_name_matching_the_whole_query_with_its_comma_ranks_first(index):
    # the state is named exactly "Washington"; DC's alternate is "Washington, DC"
    assert (await ids(index, "washington, dc"))[:2] == ["USA.9.1", "USA.48"]


@pytest.mark.asyncio
async def test_accents_and_case_are_ignored(index):
    assert (await ids(index, "  PARÁ "))[0] == "BRA.14"


@pytest.mark.asyncio
async def test_alternate_names_find_the_aoi_by_its_name(index):
    matches = await index.search("grao-para", aoi_types=None, limit=1)

    assert [(match.aoi_id, match.name) for match in matches] == [("BRA.14", "Pará")]


@pytest.mark.asyncio
async def test_any_word_of_a_name_can_match(index):
    assert (await ids(index, "amazonia"))[0] == "4"


@pytest.mark.asyncio
async def test_only_the_requested_aoi_types_are_searched(index):
    assert await ids(index, "par", aoi_types=["protected_area"]) == [
        "975",
        "4",
        "1",
        "902780",
    ]


@pytest.mark.asyncio
async def test_every_word_must_match_so_shared_words_alone_do_not(index):
    # every park ends in "National Park"; only one has a word starting "amazon"
    assert await ids(index, "amazon national park") == ["4"]


@pytest.mark.asyncio
async def test_misspelled_words_are_corrected_to_the_nearest_known_word(index):
    assert (await ids(index, "yosemte"))[0] == "975"


@pytest.mark.asyncio
async def test_a_users_own_aois_rank_above_other_aois_with_similar_names(index):
    matches = await index.search(
        "par", aoi_types=None, limit=3, custom_aois=[CustomAoiName("c1", "Parkside")]
    )

    assert (matches[0].aoi_type, matches[0].aoi_id, matches[0].name) == (
        "custom_aoi",
        "c1",
        "Parkside",
    )


@pytest.mark.asyncio
async def test_custom_aois_are_only_searched_when_given(index):
    assert await ids(index, "parkside") == []


@pytest.mark.asyncio
async def test_custom_aois_follow_the_type_filter(index):
    matches = await index.search(
        "parkside",
        aoi_types=["admin"],
        limit=3,
        custom_aois=[CustomAoiName("c1", "Parkside")],
    )

    assert matches == []
