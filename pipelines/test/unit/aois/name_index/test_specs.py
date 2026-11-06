import pyarrow as pa
from pipelines.aois.name_index.name_records import name_records
from pipelines.aois.name_index.specs import name_specs


def test_each_admin_level_is_named_with_its_own_name_and_its_parents():
    gadm = pa.table(
        {
            "aoi_id": ["CHE.18_1", "CHE.18.3.4_1"],
            "adm_level": [1, 3],
            "area_ha": [9.0e4, 1.0e3],
            "is_disputed": [False, False],
            "name_0": ["Switzerland", "Switzerland"],
            "name_1": ["Schwyz", "Schwyz"],
            "name_2": [None, "Höfe"],
            "name_3": [None, "Wollerau"],
            "name_4": [None, None],
            "name_5": [None, None],
            "varname_1": [None, None],
            "varname_2": [None, None],
            "varname_3": [None, "Wolrau|NA"],
            "varname_4": [None, None],
            "nl_name_1": [None, None],
            "nl_name_2": [None, None],
            "nl_name_3": [None, None],
        }
    )
    admin = [spec for spec in name_specs() if spec.aoi_type == "admin"]
    empty = pa.table(
        {
            "aoi_type": pa.array([], pa.string()),
            "aoi_id": pa.array([], pa.string()),
            "alternate_name": pa.array([], pa.string()),
        }
    )
    no_importance = pa.table(
        {
            "aoi_type": pa.array([], pa.string()),
            "aoi_id": pa.array([], pa.string()),
            "wikidata_id": pa.array([], pa.string()),
            "importance": pa.array([], pa.float64()),
        }
    )

    rows = name_records({"admin": gadm}, admin, no_importance, empty).to_pylist()

    names = sorted(
        (row["name"], row["display_name"], row["alternate_names"]) for row in rows
    )
    assert names == [
        ("Schwyz", "Schwyz, Switzerland", []),
        ("Wollerau", "Wollerau, Höfe, Schwyz, Switzerland", ["Wolrau"]),
    ]
