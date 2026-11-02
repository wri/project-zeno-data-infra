from app.domain.models.precomputed_admin_levels import PrecomputedAdminLevels
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
)

PRECOMPUTED = PrecomputedAdminLevels(deepest_level=2)


def test_admin_areas_down_to_the_deepest_level_are_covered():
    aoi = AdminAreaOfInterest(ids=["BRA", "BRA.1", "BRA.1.12"])

    assert PRECOMPUTED.covers(aoi)


def test_admin_areas_below_the_deepest_level_are_not_covered():
    assert not PRECOMPUTED.covers(AdminAreaOfInterest(ids=["BRA.1.12.3"]))


def test_one_deeper_id_means_the_request_is_not_covered():
    aoi = AdminAreaOfInterest(ids=["BRA.1", "BRA.1.12.3"])

    assert not PRECOMPUTED.covers(aoi)


def test_other_area_types_are_not_covered():
    assert not PRECOMPUTED.covers(ProtectedAreaOfInterest(ids=["555625448"]))
