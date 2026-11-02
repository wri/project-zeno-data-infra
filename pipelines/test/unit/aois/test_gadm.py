import geopandas as gpd
import pandas as pd
import pytest
from pipelines.aois.gadm import (
    cached_download,
    drop_unnamed_russian_units,
    fill_gadm_names,
    fix_gadm_gids,
    gadm_ids,
    mark_disputed,
    read_gadm_levels,
)
from shapely import box

GID_COLUMNS = [f"GID_{level}" for level in range(6)]


def gids(level, *values):
    row = dict.fromkeys(GID_COLUMNS) | dict(zip(GID_COLUMNS, values))
    return pd.DataFrame([{"ADM_LEVEL": level, **row}])


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


def test_hong_kong_and_macau_gids_are_fixed_at_every_level():
    row = gids(
        5, "CHN", "CHN.MAC", "MAC.1_1", "MAC.1.1_1", "MAC.1.1.2_1", "MAC.1.1.2.3_1"
    )

    fixed = fix_gadm_gids(row).iloc[0]

    assert fixed[GID_COLUMNS[2:]].tolist() == [
        "CHN.999.1_1",
        "CHN.999.1.1_1",
        "CHN.999.1.1.2_1",
        "CHN.999.1.1.2.3_1",
    ]


def test_gadm_ids_use_each_rows_own_level_without_version_suffix():
    attributes = pd.concat(
        [
            gids(0, "FRA"),
            gids(1, "FRA", "FRA.1_1"),
            gids(2, "FRA", "FRA.1_1", "FRA.1.12_1"),
            gids(3, "FRA", "FRA.1_1", "FRA.1.12_1", "FRA.1.12.3_1"),
            gids(4, "FRA", "FRA.1_1", "FRA.1.12_1", "FRA.1.12.3_1", "FRA.1.12.3.4_2"),
            gids(
                5,
                "FRA",
                "FRA.1_1",
                "FRA.1.12_1",
                "FRA.1.12.3_1",
                "FRA.1.12.3.4_2",
                "FRA.1.12.3.4.5_1",
            ),
        ],
        ignore_index=True,
    ).rename(columns=str.lower)

    assert list(gadm_ids(attributes)) == [
        "FRA",
        "FRA.1",
        "FRA.1.12",
        "FRA.1.12.3",
        "FRA.1.12.3.4",
        "FRA.1.12.3.4.5",
    ]


def test_unaddressable_na_gadm_ids_are_missing():
    attributes = pd.DataFrame(
        {"adm_level": [0], "gid_0": ["NA"], "gid_1": [None], "gid_2": [None]}
    )

    assert gadm_ids(attributes).isna().all()


def test_levels_are_read_from_their_own_layers(tmp_path):
    path = str(tmp_path / "gadm_410-levels.gpkg")
    for level in range(6):
        gpd.GeoDataFrame(
            {"GID_0": ["BRA"], "COUNTRY": ["Brazil"]},
            geometry=[box(0, 0, 1, 1)],
            crs="EPSG:4326",
        ).to_file(path, layer=f"ADM_{level}")

    levels = read_gadm_levels(path)

    assert list(levels.ADM_LEVEL) == [0, 1, 2, 3, 4, 5]
    assert list(levels.NAME_0) == ["Brazil"] * 6


def districts(name_2):
    return gids(2, "GBR", "GBR.1_1", "GBR.1.1_1").assign(NAME_2=[name_2])


def fills(aoi_id="GBR.1.1", column="NAME_2", name="Barnsley"):
    return pd.DataFrame({"aoi_id": [aoi_id], "column": [column], "name": [name]})


def test_missing_names_are_filled_from_the_lookup_table():
    assert fill_gadm_names(districts("NA"), fills()).NAME_2.tolist() == ["Barnsley"]


def test_names_gadm_already_has_are_never_overwritten():
    filled = fill_gadm_names(districts("Barnsley District"), fills())

    assert filled.NAME_2.tolist() == ["Barnsley District"]


def test_fills_for_unknown_ids_fail_loudly():
    with pytest.raises(ValueError, match="GBR.9.9"):
        fill_gadm_names(districts("NA"), fills(aoi_id="GBR.9.9"))


def units(*gids):
    return pd.DataFrame(
        [dict(zip(GID_COLUMNS, unit_gids)) for unit_gids in gids]
    ).reindex(columns=GID_COLUMNS)


def pieces(*rows):
    """Each row is a piece's GIDs followed by its DISPUTEDBY value."""
    return pd.DataFrame(
        [{**dict(zip(GID_COLUMNS, row[:-1])), "DISPUTEDBY": row[-1]} for row in rows]
    ).reindex(columns=[*GID_COLUMNS, "DISPUTEDBY"])


def test_a_unit_is_disputed_only_if_all_its_pieces_are():
    all_in_one = pieces(
        ("Z01", "Z01.1_1", "Z01.1.1_1", "China, India"),
        ("Z01", "Z01.1_1", "Z01.1.2_1", "China, India"),
        ("IND", "IND.1_1", "IND.1.1_1", "Pakistan"),
        ("IND", "IND.1_1", "IND.1.2_1", None),
    )

    marked = mark_disputed(units(("Z01",), ("IND", "IND.1_1")), all_in_one)

    assert marked.is_disputed.tolist() == [True, False]


def test_units_without_any_pieces_are_not_disputed():
    marked = mark_disputed(units(("XXX",)), pieces(("Z01", None, None, "India")))

    assert marked.is_disputed.tolist() == [False]


def test_pieces_are_matched_on_the_units_own_gids():
    all_in_one = pieces(
        ("IND", "IND.1_1", "IND.1.1_1", "Pakistan"),
        ("IND", "IND.1_1", "IND.1.2_1", None),
    )

    marked = mark_disputed(units(("IND", "IND.1_1", "IND.1.1_1")), all_in_one)

    assert marked.is_disputed.tolist() == [True]


def test_deeper_units_are_matched_on_all_their_gids():
    all_in_one = pieces(
        ("IND", "IND.1_1", "IND.1.1_1", "IND.1.1.1_1", "Pakistan"),
        ("IND", "IND.1_1", "IND.1.1_1", "IND.1.1.2_1", None),
    )

    marked = mark_disputed(
        units(("IND", "IND.1_1", "IND.1.1_1", "IND.1.1.1_1")), all_in_one
    )

    assert marked.is_disputed.tolist() == [True]


def named_units(*rows):
    return pd.DataFrame(rows, columns=["ADM_LEVEL", "GID_0", "NAME_0", "NAME_3"])


def test_unnamed_units_are_dropped_only_in_russia():
    features = named_units(
        (3, "RUS", "Russia", "NA"),
        (3, "RUS", "Russia", "Kazan"),
        (3, "THA", "Thailand", "NA"),
    )

    kept = drop_unnamed_russian_units(features)

    assert kept.NAME_3.tolist() == ["Kazan", "NA"]


def test_source_files_come_from_the_cache_once_downloaded(tmp_path):
    source = tmp_path / "source.zip"
    source.write_bytes(b"gadm")
    cache_uri = str(tmp_path / "cache" / "source.zip")

    cached_download(source.as_uri(), cache_uri, tmp_path / "first.zip")
    source.unlink()
    cached_download(source.as_uri(), cache_uri, tmp_path / "second.zip")

    assert (tmp_path / "second.zip").read_bytes() == b"gadm"
