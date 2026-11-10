from typing import List, Set

import duckdb

from app.domain.repositories.precomputed_table import (
    PrecomputedCoverage,
    sql_string_list,
)


class ResultsTableCoverage(PrecomputedCoverage):
    """Covers the AOI ids that have rows in a precomputed results table.

    A table holds rows only for AOIs with alerts, so an AOI without rows either had
    none or was saved after the table was built. Both are left out, to be computed
    on the fly.
    """

    def __init__(self, query_service):
        self.query_service = query_service

    async def select_precomputed_ids(self, aoi_ids: List[str]) -> Set[str]:
        try:
            rows = await self.query_service.execute(
                "SELECT DISTINCT aoi_id FROM data_source "
                f"WHERE aoi_id IN ({sql_string_list(aoi_ids)})"
            )
        except duckdb.IOException:
            # A version can be served before its results table is written.
            return set()
        return set(rows["aoi_id"])
