import pandas as pd
import pytest
from pydantic import ValidationError

from app.domain.analyzers.land_ghg_inventory_analyzer import (
    INPUT_URIS,
    LandGHGInventoryAnalyzer,
)
from app.domain.models.analysis import Analysis
from app.domain.models.environment import Environment
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
)
from app.models.land_change.land_ghg_inventory import LandGHGInventoryAnalyticsIn

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


@pytest.fixture
def precomputed_gadm_results(tmp_path):
    """Real precomputed zonal-statistics parquet, queried through the real adapter."""
    df = pd.DataFrame(
        {
            "aoi_id": ["BRA.1", "BRA.1", "COL", "PER"],
            "land_state_class": ["tree_loss", "tree_gain", "tree_loss", "tree_loss"],
            "year": [2016, 2016, 2016, 2016],
            "gross_emissions_MgCO2e": [100.0, 0.0, 50.0, 5.0],
            "gross_removals_MgCO2": [-10.0, -20.0, -5.0, -1.0],
            "net_flux_MgCO2e": [90.0, -20.0, 45.0, 4.0],
            "area_ha": [1.0, 2.0, 3.0, 4.0],
        }
    )
    parquet_file = tmp_path / "land_ghg_inventory_data.parquet"
    df.to_parquet(parquet_file, index=False)
    return parquet_file


@pytest.mark.asyncio
async def test_admin_query_returns_flux_by_land_state_and_year(
    precomputed_gadm_results,
):
    analytics_in = LandGHGInventoryAnalyticsIn(
        aoi=AdminAreaOfInterest(ids=["BRA.1", "COL"])
    ).model_dump()
    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)
    analyzer = LandGHGInventoryAnalyzer(
        duckdb_query_service=DuckDbPrecalcQueryService(
            table_uri=precomputed_gadm_results
        ),
        input_uris=INPUT_URIS[Environment.production],
    )

    await analyzer.analyze(analysis)

    result = pd.DataFrame(analysis.result)
    assert EXPECTED_COLUMNS.issubset(result.columns)
    # only the requested admin ids come back (PER is filtered out by the WHERE clause)
    assert set(result.aoi_id) == {"BRA.1", "COL"}
    assert set(result.aoi_type) == {"admin"}
    # tree_gain keeps its structural zero emissions (dense output, not NaN)
    tree_gain = result[
        (result.aoi_id == "BRA.1") & (result.land_state_class == "tree_gain")
    ].iloc[0]
    assert tree_gain.gross_emissions_MgCO2e == 0.0
    assert tree_gain.net_flux_MgCO2e == -20.0


def test_rejects_non_admin_aoi():
    # Admin areas only: a non-admin AOI must fail validation, not fall through to OTF.
    with pytest.raises(ValidationError):
        LandGHGInventoryAnalyticsIn(
            aoi=KeyBiodiversityAreaOfInterest(
                type="key_biodiversity_area", ids=["8111"]
            )
        )
