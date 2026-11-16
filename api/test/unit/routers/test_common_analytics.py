import pytest

from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    SavedCustomAreaOfInterest,
)
from app.routers.common_analytics import reject_unsaved_custom_aois


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
