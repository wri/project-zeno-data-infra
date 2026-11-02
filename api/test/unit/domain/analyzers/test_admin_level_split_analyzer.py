from typing import Dict

import pytest

from app.domain.analyzers.admin_level_split_analyzer import AdminLevelSplitAnalyzer
from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.precomputed_admin_levels import PrecomputedAdminLevels


class RecordingAnalyzer(Analyzer):
    """Returns one row per requested id, with the extra columns of
    `extra_columns_for` when the request contains that id."""

    def __init__(self, extra_columns_for: str | None = None):
        self.input_uris = {"admin_results_uri": "s3://results.parquet"}
        self.requested_ids: list[list[str]] = []
        self.extra_columns_for = extra_columns_for

    async def analyze(self, analysis: Analysis) -> None:
        ids = analysis.metadata["aoi"]["ids"]
        self.requested_ids.append(ids)
        result: Dict[str, list] = {"aoi_id": ids, "area_ha": [1.0] * len(ids)}
        if self.extra_columns_for in ids:
            result["extra"] = [0.0] * len(ids)
        analysis.result = result


def admin_analysis(*ids: str) -> Analysis:
    metadata = {"aoi": {"type": "admin", "ids": list(ids)}, "start_year": "2020"}
    return Analysis(result=None, metadata=metadata, status="pending")


def split_analyzer(inner: Analyzer) -> AdminLevelSplitAnalyzer:
    return AdminLevelSplitAnalyzer(inner, PrecomputedAdminLevels(deepest_level=2))


@pytest.mark.asyncio
async def test_requests_within_one_mode_reach_the_analyzer_unchanged():
    inner = RecordingAnalyzer()
    analysis = admin_analysis("BRA.1.2.3", "BRA.1.2.4")

    await split_analyzer(inner).analyze(analysis)

    assert inner.requested_ids == [["BRA.1.2.3", "BRA.1.2.4"]]
    assert analysis.result["aoi_id"] == ["BRA.1.2.3", "BRA.1.2.4"]


@pytest.mark.asyncio
async def test_mixed_levels_are_analyzed_separately_and_combined():
    inner = RecordingAnalyzer()
    analysis = admin_analysis("BRA.1", "BRA.1.2.3", "BRA.2")

    await split_analyzer(inner).analyze(analysis)

    assert sorted(inner.requested_ids) == [["BRA.1", "BRA.2"], ["BRA.1.2.3"]]
    assert sorted(analysis.result["aoi_id"]) == ["BRA.1", "BRA.1.2.3", "BRA.2"]
    assert analysis.result["area_ha"] == [1.0, 1.0, 1.0]
    assert analysis.metadata["aoi"]["ids"] == ["BRA.1", "BRA.1.2.3", "BRA.2"]


@pytest.mark.asyncio
async def test_mixed_levels_with_different_result_columns_fail():
    analysis = admin_analysis("BRA.1", "BRA.1.2.3")

    with pytest.raises(ValueError, match="columns"):
        await split_analyzer(RecordingAnalyzer("BRA.1.2.3")).analyze(analysis)


def test_results_are_identified_by_the_wrapped_analyzer():
    inner = RecordingAnalyzer()

    assert split_analyzer(inner).thumbprint() == inner.thumbprint()
