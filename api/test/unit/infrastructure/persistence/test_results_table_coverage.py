import pandas as pd
import pytest

from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.infrastructure.persistence.results_table_coverage import (
    ResultsTableCoverage,
)


@pytest.fixture
def results_table(tmp_path):
    path = tmp_path / "custom-integrated-alerts.parquet"
    pd.DataFrame(
        {"aoi_id": ["a", "a", "o'brien"], "area_ha": [1.0, 2.0, 3.0]}
    ).to_parquet(path, index=False)
    return path


@pytest.mark.asyncio
async def test_covers_only_ids_with_rows(results_table):
    coverage = ResultsTableCoverage(DuckDbPrecalcQueryService(table_uri=results_table))

    covered = await coverage.select_covered_ids(["a", "o'brien", "saved-later"])

    assert covered == {"a", "o'brien"}


@pytest.mark.asyncio
async def test_missing_results_table_covers_nothing(tmp_path):
    coverage = ResultsTableCoverage(
        DuckDbPrecalcQueryService(table_uri=tmp_path / "missing.parquet")
    )

    assert await coverage.select_covered_ids(["a"]) == set()
