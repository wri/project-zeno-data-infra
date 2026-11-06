import pyarrow as pa
import pytest
from pipelines.aois.name_index.name_records import NameSpec, name_records


@pytest.fixture
def records():
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
            "aoi_id": ["BRA.14", "BRA.18", "XXX.1", "BRA.99"],
            "area_ha": [1.2e8, 2.0e7, 1.0, 5.0],
            "name_0": ["Brazil", "Brazil", "Ex", "Brazil"],
            "name_1": ["Pará", "Paraná", "Para", None],
            "varname_1": ["Grão-Pará", None, None, None],
            "is_disputed": [False, False, True, False],
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
            "alternate_name": ["Grão-Pará", "  ", "State of Pará", "Ex land"],
        }
    )
    rows = name_records(
        {"admin": aois}, [spec], importance, alternate_names
    ).to_pylist()
    return {row["aoi_id"]: row for row in rows}


def test_excluded_and_unnamed_aois_are_left_out(records):
    assert set(records) == {"BRA.14", "BRA.18"}


def test_alternate_names_from_the_source_and_wikipedia_are_kept_once_each(records):
    assert sorted(records["BRA.14"]["alternate_names"]) == [
        "Grão-Pará",
        "State of Pará",
    ]


def test_aois_without_a_wikipedia_importance_fall_back_to_their_types(records):
    assert (records["BRA.14"]["importance"], records["BRA.18"]["importance"]) == (
        0.7,
        0.45,
    )
