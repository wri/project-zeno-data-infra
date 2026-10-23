import pytest

from pipelines.aois.sources import aoi_sources, select_aoi_sources


def test_all_aoi_sources_are_built_when_none_is_named():
    selected = select_aoi_sources(aoi_sources(), None)

    assert [source.source for source in selected] == [
        "gadm",
        "wdpa",
        "kba",
        "landmark",
    ]


def test_only_the_named_aoi_source_is_built():
    selected = select_aoi_sources(aoi_sources(), "kba")

    assert [source.source for source in selected] == ["kba"]


def test_unknown_aoi_source_lists_valid_names():
    with pytest.raises(ValueError, match="gadm, wdpa, kba, landmark"):
        select_aoi_sources(aoi_sources(), "kbas")
