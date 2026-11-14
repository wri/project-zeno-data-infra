import pandas as pd
from pipelines.aois.name_index.namers.gadm_names import GadmNames


def test_each_admin_level_is_named_with_its_own_name_and_its_parents():
    gadm = pd.DataFrame(
        {
            "aoi_id": ["CHE.18_1", "CHE.18.3.4_1", "XXX.1_1"],
            "adm_level": [1, 3, 1],
            "area_ha": [9.0e4, 1.0e3, 1.0],
            "is_disputed": [False, False, True],
            "name_0": ["Switzerland", "Switzerland", "Ex"],
            "name_1": ["Schwyz", "Schwyz", "Disputed"],
            "name_2": [None, "Höfe", None],
            "name_3": [None, "Wollerau", None],
            "varname_1": [None, None, None],
            "varname_3": [None, "Wolrau|NA", None],
            "nl_name_1": [None, None, None],
        }
    )

    names = GadmNames().names(gadm).query("aoi_id != 'global'")

    assert names[["name", "display_name", "alternate_names"]].values.tolist() == [
        ["Schwyz", "Schwyz, Switzerland", []],
        ["Wollerau", "Wollerau, Höfe, Schwyz, Switzerland", ["Wolrau"]],
    ]
    assert names["fallback_importance"].tolist() == [0.45, 0.14]


def test_the_world_is_named_global_with_its_alternate_names():
    gadm = pd.DataFrame(
        {
            "aoi_id": ["CHE", "FRA", "CHE.18_1"],
            "adm_level": [0, 0, 1],
            "area_ha": [4.0e6, 5.0e7, 9.0e4],
            "is_disputed": [False, False, False],
            "name_0": ["Switzerland", "France", "Switzerland"],
            "name_1": [None, None, "Schwyz"],
        }
    )

    names = GadmNames(global_alternate_names=("World", "Earth")).names(gadm)

    world = names.set_index("aoi_id").loc["global"]
    assert [world["name"], world["display_name"]] == ["Global", "Global"]
    assert world["alternate_names"] == ["World", "Earth"]
    assert world["area_ha"] == 5.4e7
