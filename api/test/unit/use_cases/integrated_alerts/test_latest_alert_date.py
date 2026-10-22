import pandas as pd
import pytest

from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.use_cases.integrated_alerts.latest_alert_date import (
    IntegratedAlertsLatestDate,
    LatestAlertDateNotFound,
)


@pytest.mark.asyncio
async def test_returns_latest_alert_date_across_all_row_groups(tmp_path):
    # The latest date sits in a non-final row group, for a different AOI.
    df = pd.DataFrame(
        {
            "aoi_id": ["BRA.1", "BRA.1", "IDN.2", "IDN.2"],
            "alert_date": pd.to_datetime(
                ["2024-01-01", "2024-12-31", "2025-03-04", "2024-06-15"]
            ),
            "area_ha": [1.0, 2.0, 3.0, 4.0],
        }
    )
    parquet_file = tmp_path / "admin-integrated-alerts.parquet"
    df.to_parquet(parquet_file, index=False, row_group_size=1)

    latest_date = IntegratedAlertsLatestDate(
        DuckDbPrecalcQueryService(table_uri=parquet_file)
    )

    assert await latest_date.get() == "2025-03-04"


@pytest.mark.asyncio
async def test_raises_when_parquet_has_no_alert_dates(tmp_path):
    df = pd.DataFrame({"alert_date": pd.to_datetime([]), "area_ha": []})
    parquet_file = tmp_path / "admin-integrated-alerts.parquet"
    df.to_parquet(parquet_file, index=False)

    latest_date = IntegratedAlertsLatestDate(
        DuckDbPrecalcQueryService(table_uri=parquet_file)
    )

    with pytest.raises(LatestAlertDateNotFound):
        await latest_date.get()
