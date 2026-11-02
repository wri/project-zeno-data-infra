from app.domain.models.precomputed_admin_levels import has_precomputed_results
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
)


def test_admin_areas_down_to_level_2_have_precomputed_results():
    aoi = AdminAreaOfInterest(ids=["BRA", "BRA.1", "BRA.1.12"])

    assert has_precomputed_results(aoi)


def test_admin_areas_below_level_2_have_no_precomputed_results():
    assert not has_precomputed_results(AdminAreaOfInterest(ids=["BRA.1.12.3"]))


def test_other_area_types_have_no_precomputed_results():
    assert not has_precomputed_results(ProtectedAreaOfInterest(ids=["555625448"]))
