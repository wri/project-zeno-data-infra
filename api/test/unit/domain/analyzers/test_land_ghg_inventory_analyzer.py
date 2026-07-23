from typing import Dict
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest
from pydantic import ValidationError

from app.domain.analyzers.land_ghg_inventory_analyzer import (
    INPUT_URIS,
    LandGhgInventoryAnalyzer,
)
from app.domain.models.analysis import Analysis
from app.domain.models.environment import Environment
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
)
from app.models.land_change.land_ghg_inventory import LandGhgInventoryAnalyticsIn

EXPECTED_COLUMNS = {
    "aoi_id",
    "aoi_type",
    "land_state_class",
    "year",
    "gross_emissions_MgCO2e",
    "gross_removals_MgCO2",
    "net_flux_MgCO2e",
    "area_ha",
}


class FakeParquetQueryService:
    """Runs the analyzer's real SQL against an in-memory parquet stand-in."""

    async def execute(self, query: str) -> Dict:
        data_source = pd.DataFrame(  # noqa: F841 (resolved by duckdb.sql scope)
            {
                "aoi_id": ["BRA.1", "BRA.1", "COL", "PER"],
                "land_state_class": [
                    "tree_loss",
                    "tree_gain",
                    "tree_loss",
                    "tree_loss",
                ],
                "year": [2016, 2016, 2016, 2016],
                "gross_emissions_MgCO2e": [100.0, 0.0, 50.0, 5.0],
                "gross_removals_MgCO2": [-10.0, -20.0, -5.0, -1.0],
                "net_flux_MgCO2e": [90.0, -20.0, 45.0, 4.0],
                "area_ha": [1.0, 2.0, 3.0, 4.0],
            }
        )
        return duckdb.sql(query).df().to_dict(orient="list")


@pytest.mark.asyncio
async def test_admin_query_returns_flux_by_land_state_and_year():
    analytics_in = LandGhgInventoryAnalyticsIn(
        aoi=AdminAreaOfInterest(ids=["BRA.1", "COL"])
    ).model_dump()
    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)
    analyzer = LandGhgInventoryAnalyzer(input_uris=INPUT_URIS[Environment.production])

    with patch(
        "app.domain.analyzers.land_ghg_inventory_analyzer.DuckDbPrecalcQueryService"
    ) as mock_qs:
        mock_qs.return_value.execute = FakeParquetQueryService().execute
        await analyzer.analyze(analysis)

    result = pd.DataFrame(analysis.result)
    assert EXPECTED_COLUMNS.issubset(result.columns)
    # only the requested GADM ids come back (PER is filtered out by the WHERE clause)
    assert set(result.aoi_id) == {"BRA.1", "COL"}
    assert set(result.aoi_type) == {"admin"}
    # tree_gain keeps its structural zero emissions (dense output, not NaN)
    tree_gain = result[
        (result.aoi_id == "BRA.1") & (result.land_state_class == "tree_gain")
    ].iloc[0]
    assert tree_gain.gross_emissions_MgCO2e == 0.0
    assert tree_gain.net_flux_MgCO2e == -20.0


def test_rejects_non_gadm_aoi():
    # GADM areas only: a non-admin AOI must fail validation, not fall through to OTF.
    with pytest.raises(ValidationError):
        LandGhgInventoryAnalyticsIn(
            aoi=KeyBiodiversityAreaOfInterest(
                type="key_biodiversity_area", ids=["8111"]
            )
        )
