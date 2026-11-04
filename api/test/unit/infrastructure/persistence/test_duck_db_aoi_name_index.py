import json

import duckdb
import pandas as pd
import pytest

from app.domain.models.custom_aoi import CustomAoiName
from app.infrastructure.persistence.duck_db_aoi_name_index import DuckDbAoiNameIndex

WORD_SEPARATORS = r"[\s,.;:()\[\]/'\"-]+"


def aoi(aoi_type, aoi_id, name, display_name, fallback_importance, area_ha):
    return {
        "aoi_type": aoi_type,
        "aoi_id": aoi_id,
        "name": name,
        "display_name": display_name,
        "fallback_importance": fallback_importance,
        "area_ha": area_ha,
    }


def write_index(directory, aois, alternate_names, importance):
    """Writes the files the AOI pipeline produces, from AOIs, their alternate
    names and their importance."""
    directory.mkdir()
    connection = duckdb.connect()
    connection.register("aois", pd.DataFrame(aois))
    connection.register(
        "alternates",
        pd.DataFrame(alternate_names, columns=["aoi_type", "aoi_id", "name"]),
    )
    connection.register(
        "raw_importance",
        pd.DataFrame(importance, columns=["aoi_type", "aoi_id", "importance"]),
    )
    connection.execute(
        "CREATE TABLE names AS SELECT *,"
        " strip_accents(lower(display_name)) AS search_display_name FROM aois"
    )
    connection.execute("CREATE TABLE importance AS SELECT * FROM raw_importance")
    connection.execute(
        """
        CREATE TABLE search_texts AS
        SELECT row_number() OVER () AS text_id, aoi_type, aoi_id, is_alternate,
            strip_accents(lower(trim(name))) AS text
        FROM (
            SELECT aoi_type, aoi_id, name, false AS is_alternate FROM aois
            UNION ALL
            SELECT aoi_type, aoi_id, name, true FROM alternates
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE text_words AS
        SELECT DISTINCT text_id, word
        FROM (
            SELECT text_id, unnest(regexp_split_to_array(text, $separators)) AS word
            FROM search_texts
        )
        WHERE word <> ''
        """,
        {"separators": WORD_SEPARATORS},
    )
    for table in ("names", "importance", "search_texts", "text_words"):
        connection.execute(f"COPY {table} TO '{directory / table}.parquet'")
    (directory / "metadata.json").write_text(
        json.dumps({"word_separators": WORD_SEPARATORS})
    )


@pytest.fixture
def index(tmp_path):
    directory = tmp_path / "index"
    write_index(
        directory,
        [
            aoi("admin", "PRY", "Paraguay", "Paraguay", 0.5, 4.0e7),
            aoi("admin", "BRA.14", "Pará", "Pará, Brazil", 0.45, 1.2e8),
            aoi("admin", "BRA.18", "Paraná", "Paraná, Brazil", 0.45, 2.0e7),
            aoi("admin", "SUR.6", "Para", "Para, Suriname", 0.45, 1.5e5),
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
                0.45,
                1.8e7,
            ),
            aoi(
                "admin",
                "USA.9.1",
                "District of Columbia",
                "District of Columbia, District of Columbia, United States",
                0.35,
                1.8e4,
            ),
            aoi("protected_area", "1", "Pari", "Pari, BRA", 0.15, 10.0),
            aoi(
                "protected_area",
                "975",
                "Yosemite National Park",
                "Yosemite National Park, USA",
                0.15,
                3.0e5,
            ),
            aoi(
                "protected_area",
                "4",
                "Parque Nacional da Amazônia",
                "Parque Nacional da Amazônia, BRA",
                0.15,
                1.0e6,
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
        [
            ("admin", "BRA.14", "Grão-Pará"),
            ("admin", "USA.9.1", "Washington"),
            ("admin", "USA.9.1", "Washington, DC"),
            ("protected_area", "4", "Amazonia National Park"),
            ("protected_area", "975", "Parc national de Yosemite"),
        ],
        [
            ("admin", "PRY", 0.77),
            ("admin", "BRA.14", 0.65),
            ("admin", "BRA.18", 0.68),
            ("admin", "SUR.6", 0.44),
            ("admin", "USA.48", 0.76),
            ("admin", "USA.9.1", 0.81),
            ("protected_area", "975", 0.59),
        ],
    )
    return DuckDbAoiNameIndex(str(directory))


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
