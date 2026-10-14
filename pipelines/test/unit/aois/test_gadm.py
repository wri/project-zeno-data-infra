import geopandas as gpd
import pandas as pd
import pytest
from shapely import box

from pipelines.aois.gadm import fix_gadm_gids, gadm_ids, read_gadm_levels


def gids(level, gid_0, gid_1=None, gid_2=None):
    return pd.DataFrame(
        {"ADM_LEVEL": [level], "GID_0": [gid_0], "GID_1": [gid_1], "GID_2": [gid_2]}
    )


@pytest.mark.parametrize(
    "row, expected_gid_1, expected_gid_2",
    [
        (gids(1, "CHN", "CHN.HKG"), "CHN.998_1", None),
        (gids(1, "CHN", "CHN.MAC"), "CHN.999_1", None),
        (gids(2, "CHN", "CHN.HKG", "HKG.3_1"), "CHN.998_1", "CHN.998.3_1"),
        (gids(2, "CHN", "CHN.MAC", "MAC.2_1"), "CHN.999_1", "CHN.999.2_1"),
        (gids(1, "UKR", "?"), "UKR.999_1", None),
        (gids(2, "UKR", "?", "?"), "UKR.999_1", "UKR.999.999_1"),
        (gids(2, "GBR", "NA", "GBR.1.21_1"), "GBR.1_1", "GBR.1.21_1"),
        (gids(1, "GHA", "GHA15_2"), "GHA.15_2", None),
        (gids(2, "GHA", "GHA15_2", "GHA15.3_2"), "GHA.15_2", "GHA.15.3_2"),
        (gids(2, "BRA", "BRA.1_1", "BRA.1.12_1"), "BRA.1_1", "BRA.1.12_1"),
    ],
)
def test_gids_are_fixed_like_the_data_api_preprocessing(
    row, expected_gid_1, expected_gid_2
):
    fixed = fix_gadm_gids(row).iloc[0]

    assert fixed.GID_1 == expected_gid_1
    assert fixed.GID_2 == expected_gid_2


def test_gadm_ids_use_each_rows_own_level_without_version_suffix():
    attributes = pd.DataFrame(
        {
            "adm_level": [0, 1, 2],
            "gid_0": ["BRA", "BRA", "BRA"],
            "gid_1": [None, "BRA.1_1", "BRA.1_1"],
            "gid_2": [None, None, "BRA.1.12_1"],
        }
    )

    assert list(gadm_ids(attributes)) == ["BRA", "BRA.1", "BRA.1.12"]


def test_unaddressable_na_gadm_ids_are_missing():
    attributes = pd.DataFrame({"adm_level": [0], "gid_0": ["NA"]})

    assert gadm_ids(attributes).isna().all()


def test_levels_are_read_from_their_own_layers(tmp_path):
    path = str(tmp_path / "gadm_410-levels.gpkg")
    for level, columns in enumerate(
        [
            {"GID_0": ["BRA"], "COUNTRY": ["Brazil"]},
            {"GID_0": ["BRA"], "COUNTRY": ["Brazil"], "GID_1": ["BRA.1_1"]},
        ]
    ):
        gpd.GeoDataFrame(columns, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326").to_file(
            path, layer=f"ADM_{level}"
        )

    levels = read_gadm_levels(path, levels=(0, 1))

    assert list(levels.ADM_LEVEL) == [0, 1]
    assert list(levels.NAME_0) == ["Brazil", "Brazil"]
