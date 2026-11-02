import asyncio
import copy
from typing import Any, Dict

import pandas as pd

from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis
from app.domain.models.precomputed_admin_levels import PrecomputedAdminLevels


class AdminLevelSplitAnalyzer(Analyzer):
    """Lets an analyzer take admin requests that mix precomputed and deeper
    levels: the two groups of ids are analyzed separately, at the same time,
    and their result rows combined. Any other request goes to the analyzer
    unchanged."""

    def __init__(self, analyzer: Analyzer, precomputed: PrecomputedAdminLevels):
        self.analyzer = analyzer
        self.precomputed = precomputed
        self.input_uris = analyzer.input_uris

    async def analyze(self, analysis: Analysis) -> None:
        aoi = analysis.metadata["aoi"]
        if aoi["type"] != "admin":
            await self.analyzer.analyze(analysis)
            return

        precomputed_ids = [i for i in aoi["ids"] if self.precomputed.includes(i)]
        on_the_fly_ids = [i for i in aoi["ids"] if not self.precomputed.includes(i)]
        if not precomputed_ids or not on_the_fly_ids:
            await self.analyzer.analyze(analysis)
            return

        parts = [
            with_ids(analysis, precomputed_ids),
            with_ids(analysis, on_the_fly_ids),
        ]
        await asyncio.gather(*(self.analyzer.analyze(part) for part in parts))

        for part in parts:
            analysis.metadata.update({**part.metadata, "aoi": aoi})
        analysis.result = combined_rows(parts[0].result, parts[1].result)


def with_ids(analysis: Analysis, ids: list[str]) -> Analysis:
    metadata = copy.deepcopy(analysis.metadata)
    metadata["aoi"]["ids"] = ids
    return Analysis(result=None, metadata=metadata, status=analysis.status)


def combined_rows(first: Dict[str, Any], second: Dict[str, Any]) -> Dict[str, list]:
    """Rows of two column-oriented results. A column given as a single value
    applies to all of its result's rows."""
    if set(first) != set(second):
        raise ValueError(
            "Precomputed and on-the-fly results have different columns: "
            f"{sorted(set(first) ^ set(second))}"
        )
    rows = pd.concat([pd.DataFrame(first), pd.DataFrame(second)], ignore_index=True)
    return rows.to_dict(orient="list")
