import pandas as pd
import pytest

from app.infrastructure.persistence.duck_db_aoi_name_index import (
    DuckDbAoiNameIndex,
    NameSource,
)


def admin_source(tmp_path):
    uri = str(tmp_path / "gadm.parquet")
    pd.DataFrame(
        {
            "aoi_id": ["PRY", "BRA.14", "BRA.18", "SUR.6", "USA.5.22", "XXX.1"],
            "adm_level": [0, 1, 1, 1, 2, 1],
            "name_0": [
                "Paraguay",
                "Brazil",
                "Brazil",
                "Suriname",
                "United States",
                "Ex",
            ],
            "name_1": [None, "Pará", "Paraná", "Para", "California", "Para"],
            "name_2": [None, None, None, None, "Yosemite West", None],
            "varname_1": [None, "Grão-Pará", None, None, None, None],
            "is_disputed": [False, False, False, False, False, True],
            "area_ha": [4.0e7, 1.2e8, 2.0e7, 1.5e5, 1.0e4, 1.0],
        }
    ).to_parquet(uri)
    return NameSource(
        uri=uri,
        columns=("adm_level", "name_0", "name_1", "name_2", "varname_1", "is_disputed"),
        name="CASE adm_level WHEN 0 THEN name_0 WHEN 1 THEN name_1 ELSE name_2 END",
        display_name=(
            "CASE adm_level WHEN 0 THEN name_0"
            " WHEN 1 THEN concat_ws(', ', name_1, name_0)"
            " ELSE concat_ws(', ', name_2, name_1, name_0) END"
        ),
        alternate_names="CASE adm_level WHEN 1 THEN [varname_1] ELSE [] END",
        fallback_importance=(
            "CASE adm_level WHEN 0 THEN 0.5 WHEN 1 THEN 0.45 ELSE 0.35 END"
        ),
        excluded="is_disputed",
    )


def protected_area_source(tmp_path):
    uri = str(tmp_path / "wdpa.parquet")
    pd.DataFrame(
        {
            "aoi_id": ["1", "975", "4", "902780"],
            "name": [
                "Pari",
                "Yosemite National Park",
                "Parque Nacional da Amazônia",
                "Oulanka National Park",
            ],
            "name_eng": [None, None, "Amazonia National Park", None],
            "iso3": ["BRA", "USA", "BRA", "FIN"],
            "area_ha": [10.0, 300000.0, 1000000.0, 27000.0],
        }
    ).to_parquet(uri)
    return NameSource(
        uri=uri,
        columns=("name", "name_eng", "iso3"),
        name="name",
        display_name="concat_ws(', ', name, iso3)",
        alternate_names="[name_eng]",
        fallback_importance="0.15",
    )


@pytest.fixture
def index(tmp_path):
    importance_uri = str(tmp_path / "importance.parquet")
    pd.DataFrame(
        {
            "aoi_type": ["admin"] * 4 + ["protected_area"],
            "aoi_id": ["PRY", "BRA.14", "BRA.18", "SUR.6", "975"],
            "importance": [0.77, 0.65, 0.68, 0.44, 0.59],
        }
    ).to_parquet(importance_uri)
    alternate_names_uri = str(tmp_path / "alternate_names.parquet")
    pd.DataFrame(
        {
            "aoi_type": ["protected_area"],
            "aoi_id": ["975"],
            "alternate_name": ["Parc national de Yosemite"],
        }
    ).to_parquet(alternate_names_uri)
    return DuckDbAoiNameIndex(
        {
            "admin": admin_source(tmp_path),
            "protected_area": protected_area_source(tmp_path),
        },
        importance_uri=importance_uri,
        alternate_names_uri=alternate_names_uri,
    )


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
async def test_accents_and_case_are_ignored(index):
    assert (await ids(index, "  PARÁ "))[0] == "BRA.14"


@pytest.mark.asyncio
async def test_alternate_names_from_the_source_find_the_aoi(index):
    matches = await index.search("grao-para", aoi_types=None, limit=1)

    assert [(match.aoi_id, match.name) for match in matches] == [("BRA.14", "Pará")]


@pytest.mark.asyncio
async def test_alternate_names_from_the_alternate_names_file_find_the_aoi(index):
    assert (await ids(index, "parc national de yosemite"))[0] == "975"


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
async def test_excluded_areas_are_never_matched(index):
    # the disputed area is named exactly "Para"
    assert "XXX.1" not in await ids(index, "para")


@pytest.mark.asyncio
async def test_every_word_must_match_so_shared_words_alone_do_not(index):
    # every park ends in "National Park"; only one has a word starting "amazon"
    assert await ids(index, "amazon national park") == ["4"]


@pytest.mark.asyncio
async def test_misspelled_words_are_corrected_to_the_nearest_known_word(index):
    assert (await ids(index, "yosemte"))[0] == "975"
