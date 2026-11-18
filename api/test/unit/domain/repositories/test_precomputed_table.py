import pytest

from app.domain.repositories.precomputed_table import PrecomputedAdminLevels
from app.models.common.areas_of_interest import AdminAreaOfInterest


@pytest.mark.asyncio
async def test_admin_areas_down_to_the_precomputed_levels_are_precomputed():
    aoi = AdminAreaOfInterest(ids=["BRA", "BRA.1", "BRA.1.12"])

    assert await PrecomputedAdminLevels().select_precomputed_ids(aoi) == {
        "BRA",
        "BRA.1",
        "BRA.1.12",
    }


@pytest.mark.asyncio
async def test_admin_areas_below_the_precomputed_levels_are_not_precomputed():
    aoi = AdminAreaOfInterest(ids=["BRA.1.12.3"])

    assert await PrecomputedAdminLevels().select_precomputed_ids(aoi) == set()
