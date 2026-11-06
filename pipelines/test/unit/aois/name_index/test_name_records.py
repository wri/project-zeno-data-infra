import pandas as pd
import pytest

from pipelines.aois.name_index.external_importance import ExternalDetails
from pipelines.aois.name_index.name_records import name_records


class FakeNamer:
    aoi_type = "admin"
    source = "gadm"

    def names(self, aois):
        return pd.DataFrame(
            {
                "aoi_id": aois["aoi_id"],
                "name": aois["name"],
                "display_name": aois["name"],
                "alternate_names": aois["variants"],
                "fallback_importance": 0.45,
                "area_ha": 1.0,
            }
        )


@pytest.fixture
def records():
    aois = pd.DataFrame(
        {
            "aoi_id": ["BRA.14", "BRA.18", "BRA.99"],
            "name": ["Pará", "Paraná", None],
            "variants": [["Grão-Pará"], [], []],
        }
    )
    external = ExternalDetails(
        importance=pd.DataFrame(
            {"aoi_type": ["admin"], "aoi_id": ["BRA.14"], "importance": [0.7]}
        ),
        alternate_names=pd.DataFrame(
            {
                "aoi_type": ["admin", "admin", "admin"],
                "aoi_id": ["BRA.14", "BRA.14", "BRA.14"],
                "alternate_name": ["Grão-Pará", "  ", "State of Pará"],
            }
        ),
        sources={},
    )
    rows = name_records({"admin": aois}, [FakeNamer()], external)
    return {row["aoi_id"]: row for row in rows.to_dict("records")}


def test_unnamed_aois_are_left_out(records):
    assert set(records) == {"BRA.14", "BRA.18"}


def test_alternate_names_from_the_source_and_externally_are_kept_once_each(records):
    assert sorted(records["BRA.14"]["alternate_names"]) == [
        "Grão-Pará",
        "State of Pará",
    ]


def test_aois_without_an_external_importance_fall_back_to_their_types(records):
    assert (records["BRA.14"]["importance"], records["BRA.18"]["importance"]) == (
        0.7,
        0.45,
    )
