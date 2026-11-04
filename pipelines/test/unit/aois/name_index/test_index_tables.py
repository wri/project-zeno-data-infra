import pyarrow as pa
import pytest
from pipelines.aois.name_index.index_tables import NameSpec, name_index_tables

WORD_SEPARATORS = r"[\s,.;:()\[\]/'\"-]+"


@pytest.fixture
def tables():
    spec = NameSpec(
        aoi_type="admin",
        source="gadm",
        name="name_1",
        display_name="concat_ws(', ', name_1, name_0)",
        alternate_names="[varname_1]",
        fallback_importance="0.45",
        excluded="is_disputed",
    )
    aois = pa.table(
        {
            "aoi_id": ["BRA.14", "XXX.1", "BRA.99"],
            "area_ha": [1.2e8, 1.0, 5.0],
            "name_0": ["Brazil", "Ex", "Brazil"],
            "name_1": ["Pará", "Para", None],
            "varname_1": ["Grão-Pará", None, None],
            "is_disputed": [False, True, False],
        }
    )
    importance = pa.table(
        {
            "aoi_type": ["admin"],
            "aoi_id": ["BRA.14"],
            "wikidata_id": ["Q1"],
            "importance": [0.7],
        }
    )
    alternate_names = pa.table(
        {
            "aoi_type": ["admin", "admin", "admin", "admin"],
            "aoi_id": ["BRA.14", "BRA.14", "BRA.14", "XXX.1"],
            "alternate_name": ["Pará", "  ", "State of Pará", "Ex land"],
        }
    )
    return name_index_tables(
        {"admin": aois}, [spec], importance, alternate_names, WORD_SEPARATORS
    )


def test_excluded_and_unnamed_aois_are_left_out(tables):
    names = tables.names.to_pylist()

    assert [
        (row["aoi_id"], row["display_name"], row["search_display_name"])
        for row in names
    ] == [("BRA.14", "Pará, Brazil", "para, brazil")]
    assert set(tables.search_texts["aoi_id"].to_pylist()) == {"BRA.14"}


def test_names_and_alternate_names_are_normalized_search_texts_once_each(tables):
    texts = sorted(
        (row["text"], row["is_alternate"]) for row in tables.search_texts.to_pylist()
    )

    assert texts == [
        ("grao-para", True),
        ("para", False),
        ("state of para", True),
    ]


def test_words_are_split_the_same_way_as_search_queries(tables):
    words = set(tables.text_words["word"].to_pylist())

    assert words == {"para", "grao", "state", "of"}
