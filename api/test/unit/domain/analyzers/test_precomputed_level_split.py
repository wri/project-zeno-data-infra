from typing import Dict, Union

import pytest
from pydantic import BaseModel

from app.domain.analyzers.precomputed_level_split import analyze_by_precomputed_levels
from app.domain.models.precomputed_admin_levels import PrecomputedAdminLevels
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    ProtectedAreaOfInterest,
)


class Request(BaseModel):
    aoi: Union[AdminAreaOfInterest, ProtectedAreaOfInterest]
    start_year: str = "2020"


class RecordingAnalysis:
    """One row per requested id, with an extra column when the request
    contains `extra_column_for`."""

    def __init__(self, extra_column_for: str | None = None):
        self.requests: list[Request] = []
        self.extra_column_for = extra_column_for

    async def __call__(self, request: Request) -> Dict[str, list]:
        self.requests.append(request)
        ids = request.aoi.ids
        result: Dict[str, list] = {"aoi_id": ids, "area_ha": [1.0] * len(ids)}
        if self.extra_column_for in ids:
            result["extra"] = [0.0] * len(ids)
        return result


async def analyze(request, precomputed, on_the_fly):
    return await analyze_by_precomputed_levels(
        request, PrecomputedAdminLevels(deepest_level=2), precomputed, on_the_fly
    )


@pytest.mark.asyncio
async def test_precomputed_levels_are_only_analyzed_from_precomputed_results():
    precomputed, on_the_fly = RecordingAnalysis(), RecordingAnalysis()
    request = Request(aoi=AdminAreaOfInterest(ids=["BRA.1", "BRA.2.3"]))

    await analyze(request, precomputed, on_the_fly)

    assert precomputed.requests == [request]
    assert on_the_fly.requests == []


@pytest.mark.asyncio
async def test_deeper_levels_and_other_area_types_are_analyzed_on_the_fly():
    for aoi in (
        AdminAreaOfInterest(ids=["BRA.1.2.3"]),
        ProtectedAreaOfInterest(ids=["555625448"]),
    ):
        precomputed, on_the_fly = RecordingAnalysis(), RecordingAnalysis()

        await analyze(Request(aoi=aoi), precomputed, on_the_fly)

        assert precomputed.requests == []
        assert [request.aoi for request in on_the_fly.requests] == [aoi]


@pytest.mark.asyncio
async def test_mixed_levels_are_analyzed_separately_and_combined():
    precomputed, on_the_fly = RecordingAnalysis(), RecordingAnalysis()
    request = Request(aoi=AdminAreaOfInterest(ids=["BRA.1", "BRA.1.2.3", "BRA.2"]))

    result = await analyze(request, precomputed, on_the_fly)

    assert [r.aoi.ids for r in precomputed.requests] == [["BRA.1", "BRA.2"]]
    assert [r.aoi.ids for r in on_the_fly.requests] == [["BRA.1.2.3"]]
    assert on_the_fly.requests[0].start_year == "2020"
    assert result == {
        "aoi_id": ["BRA.1", "BRA.2", "BRA.1.2.3"],
        "area_ha": [1.0, 1.0, 1.0],
    }


@pytest.mark.asyncio
async def test_mixed_levels_with_different_result_columns_fail():
    request = Request(aoi=AdminAreaOfInterest(ids=["BRA.1", "BRA.1.2.3"]))

    with pytest.raises(ValueError, match="columns"):
        await analyze(request, RecordingAnalysis(), RecordingAnalysis("BRA.1.2.3"))
