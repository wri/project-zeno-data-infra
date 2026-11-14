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

    names = GadmNames().names(gadm)

    assert names[["name", "display_name", "alternate_names"]].values.tolist() == [
        ["Schwyz", "Schwyz, Switzerland", []],
        ["Wollerau", "Wollerau, Höfe, Schwyz, Switzerland", ["Wolrau"]],
    ]
    assert names["fallback_importance"].tolist() == [0.45, 0.14]
