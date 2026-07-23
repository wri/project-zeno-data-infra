import pytest

from pipelines.globals import gadm_country_code_count
from pipelines.prefect_flows.common_stages import numeric_to_alpha3
from pipelines.run_updates import UpdateFlow, _parse_bbox, _validate_flow_args


def test_parse_bbox_parses_minx_miny_maxx_maxy():
    assert _parse_bbox("-74,-11.2,-66.5,-7").bounds == (-74.0, -11.2, -66.5, -7.0)


def test_parse_bbox_is_none_when_empty():
    assert _parse_bbox(None) is None
    assert _parse_bbox("") is None


def test_parse_bbox_rejects_wrong_coordinate_count():
    with pytest.raises(ValueError):
        _parse_bbox("-74,-11.2,-66.5")


@pytest.mark.parametrize(
    "flow_name",
    [UpdateFlow.TCL_UPDATE, UpdateFlow.LAND_GHG_INVENTORY_UPDATE],
)
def test_version_required_for_versioned_flows(flow_name):
    with pytest.raises(ValueError):
        _validate_flow_args(flow_name, version=None)


def test_version_not_required_for_dist_update():
    _validate_flow_args(UpdateFlow.DIST_UPDATE, version=None)  # must not raise


def test_country_expected_groups_covers_every_iso_code():
    # flox silently drops group labels >= the expected_groups bound, so the country
    # axis must exceed the largest numeric ISO code we map.
    assert max(numeric_to_alpha3) < gadm_country_code_count
