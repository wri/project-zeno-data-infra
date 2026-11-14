import pytest

from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    SavedCustomAreaOfInterest,
)
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn
from app.routers.common_analytics import expand_global_area, reject_unsaved_custom_aois


class SavedIds:
    def __init__(self, saved):
        self.saved = saved

    async def list_saved_ids(self, aoi_ids):
        return {aoi_id for aoi_id in aoi_ids if aoi_id in self.saved}


def unreachable_repository():
    raise AssertionError("only saved custom AOIs are looked up")


@pytest.mark.asyncio
async def test_saved_custom_aois_and_other_aoi_types_are_let_through():
    await reject_unsaved_custom_aois(
        SavedCustomAreaOfInterest(ids=["a", "b"]), lambda: SavedIds({"a", "b"})
    )
    await reject_unsaved_custom_aois(
        AdminAreaOfInterest(ids=["BRA"]), unreachable_repository
    )


def tree_cover_loss_in(ids):
    return TreeCoverLossAnalyticsIn(
        aoi={"type": "admin", "ids": ids},
        start_year="2020",
        end_year="2024",
        canopy_cover=30,
        intersections=[],
    )


def unreachable_countries():
    raise AssertionError("countries are only read for the global area")


def test_the_global_area_is_analyzed_as_every_country():
    expanded = expand_global_area(
        tree_cover_loss_in(["global"]), lambda: ("BRA", "IDN")
    )

    assert expanded.aoi.ids == ["BRA", "IDN"]


def test_other_areas_are_analyzed_as_requested():
    request = tree_cover_loss_in(["BRA.14"])

    assert expand_global_area(request, unreachable_countries) == request
