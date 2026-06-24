import asyncio

import pandas as pd
import pytest

from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)


class SlowQueryService(DuckDbPrecalcQueryService):
    async def _run(self, query):
        await asyncio.sleep(10)


class FastQueryService(DuckDbPrecalcQueryService):
    async def _run(self, query):
        return pd.DataFrame({"aoi_id": ["BRA.1"], "area_ha": [1.0]})


class TestDuckDbPrecalcQueryServiceTimeout:
    @pytest.mark.asyncio
    async def test_raises_timeout_when_query_exceeds_limit(self):
        service = SlowQueryService(table_uri="x", timeout_seconds=0.01)
        with pytest.raises(asyncio.TimeoutError):
            await service.execute("SELECT 1 FROM data_source")

    @pytest.mark.asyncio
    async def test_returns_result_within_limit(self):
        service = FastQueryService(table_uri="x", timeout_seconds=5)
        result = await service.execute("SELECT 1 FROM data_source")
        assert result["aoi_id"] == ["BRA.1"]
