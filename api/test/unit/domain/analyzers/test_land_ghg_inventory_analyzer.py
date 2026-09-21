import pandas as pd
import pytest
from pydantic import ValidationError

from app.domain.analyzers.land_ghg_inventory_analyzer import (
    ANNUALIZED_YEARS,
    INPUT_URIS,
    LandGHGInventoryAnalyzer,
    admin_query,
    global_query,
)
from app.domain.models.analysis import Analysis
from app.domain.models.environment import Environment
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    GlobalAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
)
from app.models.land_change.land_ghg_inventory import LandGHGInventoryAnalyticsIn

EXPECTED_VEGETATION_COLUMNS = {
    "aoi_id",
    "aoi_type",
    "land_state_class",
    "year",
    "gross_emissions_MgCO2e",
    "gross_removals_MgCO2",
    "net_flux_MgCO2e",
    "area_ha",
}

# agriculture's static snapshot is broadcast to a year per row (2016-2024)
EXPECTED_AGRICULTURE_COLUMNS = {
    "aoi_id",
    "aoi_type",
    "category",
    "year",
    "gross_emissions_MgCO2e",
}

# mineral_soil's static snapshot is broadcast to a year per row (2016-2024)
EXPECTED_MINERAL_SOIL_COLUMNS = {
    "aoi_id",
    "aoi_type",
    "year",
    "gross_emissions_MgCO2e",
    "gross_removals_MgCO2",
    "net_flux_MgCO2e",
    "area_ha",
}

# organic_soil's two blocks are broadcast to a year per row (2016-2024)
EXPECTED_ORGANIC_SOIL_COLUMNS = {
    "aoi_id",
    "aoi_type",
    "year",
    "gross_emissions_MgCO2e",
    "area_ha",
}


@pytest.fixture
def vegetation_parquet(tmp_path):
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
    parquet_file = tmp_path / "vegetation.parquet"
    df.to_parquet(parquet_file, index=False)
    return parquet_file


@pytest.fixture
def agriculture_parquet(tmp_path):
    """Agriculture snapshot parquet; carries its own aoi_type (unlike vegetation)."""
    df = pd.DataFrame(
        {
            "category": [
                "cropland",
                "cropland",
                "cropland",
                "livestock",
                "livestock",
                "livestock",
            ],
            "gross_emissions_MgCO2e": [10.0, 5.0, 1.0, 8.0, 4.0, 2.0],
            "aoi_id": ["BRA.1", "COL", "PER", "BRA.1", "COL", "PER"],
            "aoi_type": ["admin"] * 6,
        }
    )
    parquet_file = tmp_path / "agriculture.parquet"
    df.to_parquet(parquet_file, index=False)
    return parquet_file


@pytest.fixture
def mineral_soil_parquet(tmp_path):
    """Mineral soil static snapshot parquet: one row per aoi_id, no year axis."""
    df = pd.DataFrame(
        {
            "aoi_id": ["BRA.1", "COL", "PER"],
            "aoi_type": ["admin", "admin", "admin"],
            "gross_emissions_MgCO2e": [100.0, 50.0, 5.0],
            "gross_removals_MgCO2": [-10.0, -5.0, -1.0],
            "net_flux_MgCO2e": [90.0, 45.0, 4.0],
            "area_ha": [1.0, 3.0, 4.0],
        }
    )
    parquet_file = tmp_path / "mineral_soil.parquet"
    df.to_parquet(parquet_file, index=False)
    return parquet_file


@pytest.fixture
def organic_soil_parquet(tmp_path):
    """Organic soil parquet: one row per aoi_id x interval_end_year (2020, 2024)."""
    df = pd.DataFrame(
        {
            "aoi_id": ["BRA.1", "BRA.1", "COL", "COL", "PER", "PER"],
            "aoi_type": ["admin"] * 6,
            "interval_end_year": [2020, 2024, 2020, 2024, 2020, 2024],
            "gross_emissions_MgCO2e": [10.0, 20.0, 5.0, 6.0, 1.0, 2.0],
            "area_ha": [1.0, 1.0, 3.0, 3.0, 4.0, 4.0],
        }
    )
    parquet_file = tmp_path / "organic_soil.parquet"
    df.to_parquet(parquet_file, index=False)
    return parquet_file


def build_analyzer(
    vegetation_parquet, agriculture_parquet, mineral_soil_parquet, organic_soil_parquet
):
    return LandGHGInventoryAnalyzer(
        query_services={
            "vegetation": DuckDbPrecalcQueryService(table_uri=vegetation_parquet),
            "agriculture": DuckDbPrecalcQueryService(table_uri=agriculture_parquet),
            "mineral_soil": DuckDbPrecalcQueryService(table_uri=mineral_soil_parquet),
            "organic_soil": DuckDbPrecalcQueryService(table_uri=organic_soil_parquet),
        },
        input_uris=INPUT_URIS[Environment.production],
    )


@pytest.mark.asyncio
async def test_result_has_a_table_per_category(
    vegetation_parquet, agriculture_parquet, mineral_soil_parquet, organic_soil_parquet
):
    analytics_in = LandGHGInventoryAnalyticsIn(
        aoi=AdminAreaOfInterest(ids=["BRA.1", "COL"])
    ).model_dump()
    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)

    await build_analyzer(
        vegetation_parquet,
        agriculture_parquet,
        mineral_soil_parquet,
        organic_soil_parquet,
    ).analyze(analysis)

    assert set(analysis.result) == {
        "vegetation",
        "agriculture",
        "mineral_soil",
        "organic_soil",
    }


@pytest.mark.asyncio
async def test_vegetation_query_returns_flux_by_land_state_and_year(
    vegetation_parquet, agriculture_parquet, mineral_soil_parquet, organic_soil_parquet
):
    analytics_in = LandGHGInventoryAnalyticsIn(
        aoi=AdminAreaOfInterest(ids=["BRA.1", "COL"])
    ).model_dump()
    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)

    await build_analyzer(
        vegetation_parquet,
        agriculture_parquet,
        mineral_soil_parquet,
        organic_soil_parquet,
    ).analyze(analysis)

    result = pd.DataFrame(analysis.result["vegetation"])
    assert EXPECTED_VEGETATION_COLUMNS.issubset(result.columns)
    # only the requested admin ids come back (PER is filtered out by the WHERE clause)
    assert set(result.aoi_id) == {"BRA.1", "COL"}
    assert set(result.aoi_type) == {"admin"}
    # tree_gain keeps its structural zero emissions (dense output, not NaN)
    tree_gain = result[
        (result.aoi_id == "BRA.1") & (result.land_state_class == "tree_gain")
    ].iloc[0]
    assert tree_gain.gross_emissions_MgCO2e == 0.0
    assert tree_gain.net_flux_MgCO2e == -20.0


@pytest.mark.asyncio
async def test_agriculture_query_broadcasts_snapshot_to_years(
    vegetation_parquet, agriculture_parquet, mineral_soil_parquet, organic_soil_parquet
):
    analytics_in = LandGHGInventoryAnalyticsIn(
        aoi=AdminAreaOfInterest(ids=["BRA.1", "COL"])
    ).model_dump()
    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)

    await build_analyzer(
        vegetation_parquet,
        agriculture_parquet,
        mineral_soil_parquet,
        organic_soil_parquet,
    ).analyze(analysis)

    result = pd.DataFrame(analysis.result["agriculture"])
    assert set(result.columns) == EXPECTED_AGRICULTURE_COLUMNS
    # only the requested admin ids come back (PER filtered out)
    assert set(result.aoi_id) == {"BRA.1", "COL"}
    assert set(result.category) == {"cropland", "livestock"}
    assert set(result.aoi_type) == {"admin"}
    # the static snapshot is broadcast across every vegetation year, per category
    bra_cropland = result[(result.aoi_id == "BRA.1") & (result.category == "cropland")]
    assert set(bra_cropland.year) == set(range(2016, 2025))
    assert (bra_cropland.gross_emissions_MgCO2e == 10.0).all()
    bra_livestock = result[
        (result.aoi_id == "BRA.1") & (result.category == "livestock")
    ]
    assert set(bra_livestock.year) == set(range(2016, 2025))
    assert (bra_livestock.gross_emissions_MgCO2e == 8.0).all()


@pytest.mark.asyncio
async def test_mineral_soil_query_broadcasts_snapshot_to_years(
    vegetation_parquet, agriculture_parquet, mineral_soil_parquet, organic_soil_parquet
):
    analytics_in = LandGHGInventoryAnalyticsIn(
        aoi=AdminAreaOfInterest(ids=["BRA.1", "COL"])
    ).model_dump()
    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)

    await build_analyzer(
        vegetation_parquet,
        agriculture_parquet,
        mineral_soil_parquet,
        organic_soil_parquet,
    ).analyze(analysis)

    result = pd.DataFrame(analysis.result["mineral_soil"])
    assert set(result.columns) == EXPECTED_MINERAL_SOIL_COLUMNS
    assert "interval_end_year" not in result.columns
    # only the requested admin ids come back (PER filtered out)
    assert set(result.aoi_id) == {"BRA.1", "COL"}
    assert set(result.aoi_type) == {"admin"}
    # the static snapshot is broadcast across every vegetation year
    bra = result[result.aoi_id == "BRA.1"]
    assert set(bra.year) == set(range(2016, 2025))
    assert (bra.gross_emissions_MgCO2e == 100.0).all()
    assert (bra.net_flux_MgCO2e == 90.0).all()


@pytest.mark.asyncio
async def test_organic_soil_query_broadcasts_blocks_to_years(
    vegetation_parquet, agriculture_parquet, mineral_soil_parquet, organic_soil_parquet
):
    analytics_in = LandGHGInventoryAnalyticsIn(
        aoi=AdminAreaOfInterest(ids=["BRA.1", "COL"])
    ).model_dump()
    analysis = Analysis(None, analytics_in, AnalysisStatus.saved)

    await build_analyzer(
        vegetation_parquet,
        agriculture_parquet,
        mineral_soil_parquet,
        organic_soil_parquet,
    ).analyze(analysis)

    result = pd.DataFrame(analysis.result["organic_soil"])
    assert set(result.columns) == EXPECTED_ORGANIC_SOIL_COLUMNS
    # only the requested admin ids come back (PER filtered out)
    assert set(result.aoi_id) == {"BRA.1", "COL"}
    assert set(result.aoi_type) == {"admin"}
    bra = result[result.aoi_id == "BRA.1"]
    # broadcast across every vegetation year: 2016-2020 repeats the first
    # block's value, 2021-2024 repeats the second block's value
    assert set(bra.year) == set(range(2016, 2025))
    assert (bra[bra.year <= 2020].gross_emissions_MgCO2e == 10.0).all()
    assert (bra[bra.year >= 2021].gross_emissions_MgCO2e == 20.0).all()


def test_rejects_non_admin_aoi():
    # Admin areas only: a non-admin AOI must fail validation, not fall through to OTF.
    with pytest.raises(ValidationError):
        LandGHGInventoryAnalyticsIn(
            aoi=KeyBiodiversityAreaOfInterest(
                type="key_biodiversity_area", ids=["8111"]
            )
        )


def test_rejects_global_aoi_carrying_ids():
    # The whole world is not a list of areas: ids are meaningless here, and
    # StrictBaseModel rejects them rather than silently ignoring them.
    with pytest.raises(ValidationError):
        LandGHGInventoryAnalyticsIn(aoi={"type": "global", "ids": ["BRA"]})


def test_global_and_admin_thumbprints_differ():
    # Why _version is not bumped for the global AOI: the thumbprint already
    # separates the two, so a global result can never collide with a cached
    # admin one.
    admin = LandGHGInventoryAnalyticsIn(aoi=AdminAreaOfInterest(ids=["BRA"]))
    world = LandGHGInventoryAnalyticsIn(aoi=GlobalAreaOfInterest())

    assert admin.thumbprint() != world.thumbprint()


# Nested rollup tiers, as the real parquets carry them: the adm1/adm2 rows are
# already counted inside their adm0 parent, so a global sum that fails to
# filter them out would report roughly triple the true total. The numbers below
# make that visible -- BRA's children sum to the same 100 the BRA row holds.
GLOBAL_TIER_IDS = ["BRA", "BRA.1", "BRA.1.1", "COL", "COL.1"]
# adm0 rows only: BRA + COL
EXPECTED_GLOBAL_EMISSIONS = 100.0 + 50.0


@pytest.fixture
def tiered_vegetation_parquet(tmp_path):
    df = pd.DataFrame(
        {
            "aoi_id": GLOBAL_TIER_IDS,
            "land_state_class": ["tree_loss"] * 5,
            "year": [2016] * 5,
            "gross_emissions_MgCO2e": [100.0, 60.0, 40.0, 50.0, 50.0],
            "gross_removals_MgCO2": [-10.0, -6.0, -4.0, -5.0, -5.0],
            "net_flux_MgCO2e": [90.0, 54.0, 36.0, 45.0, 45.0],
            "area_ha": [8.0, 5.0, 3.0, 4.0, 4.0],
        }
    )
    parquet_file = tmp_path / "tiered_vegetation.parquet"
    df.to_parquet(parquet_file, index=False)
    return parquet_file


@pytest.fixture
def tiered_agriculture_parquet(tmp_path):
    df = pd.DataFrame(
        {
            "aoi_id": GLOBAL_TIER_IDS * 2,
            "aoi_type": ["admin"] * 10,
            "category": ["cropland"] * 5 + ["livestock"] * 5,
            "gross_emissions_MgCO2e": [
                100.0,
                60.0,
                40.0,
                50.0,
                50.0,
                100.0,
                60.0,
                40.0,
                50.0,
                50.0,
            ],
        }
    )
    parquet_file = tmp_path / "tiered_agriculture.parquet"
    df.to_parquet(parquet_file, index=False)
    return parquet_file


@pytest.fixture
def tiered_mineral_soil_parquet(tmp_path):
    df = pd.DataFrame(
        {
            "aoi_id": GLOBAL_TIER_IDS,
            "aoi_type": ["admin"] * 5,
            "gross_emissions_MgCO2e": [100.0, 60.0, 40.0, 50.0, 50.0],
            "gross_removals_MgCO2": [-10.0, -6.0, -4.0, -5.0, -5.0],
            "net_flux_MgCO2e": [90.0, 54.0, 36.0, 45.0, 45.0],
            "area_ha": [8.0, 5.0, 3.0, 4.0, 4.0],
        }
    )
    parquet_file = tmp_path / "tiered_mineral_soil.parquet"
    df.to_parquet(parquet_file, index=False)
    return parquet_file


@pytest.fixture
def tiered_organic_soil_parquet(tmp_path):
    df = pd.DataFrame(
        {
            "aoi_id": [i for i in GLOBAL_TIER_IDS for _ in (0, 1)],
            "aoi_type": ["admin"] * 10,
            "interval_end_year": [2020, 2024] * 5,
            "gross_emissions_MgCO2e": [
                100.0,
                200.0,
                60.0,
                120.0,
                40.0,
                80.0,
                50.0,
                100.0,
                50.0,
                100.0,
            ],
            "area_ha": [8.0, 8.0, 5.0, 5.0, 3.0, 3.0, 4.0, 4.0, 4.0, 4.0],
        }
    )
    parquet_file = tmp_path / "tiered_organic_soil.parquet"
    df.to_parquet(parquet_file, index=False)
    return parquet_file


@pytest.fixture
def global_analysis():
    analytics_in = LandGHGInventoryAnalyticsIn(aoi=GlobalAreaOfInterest()).model_dump()
    return Analysis(None, analytics_in, AnalysisStatus.saved)


@pytest.fixture
def tiered_analyzer(
    tiered_vegetation_parquet,
    tiered_agriculture_parquet,
    tiered_mineral_soil_parquet,
    tiered_organic_soil_parquet,
):
    return build_analyzer(
        tiered_vegetation_parquet,
        tiered_agriculture_parquet,
        tiered_mineral_soil_parquet,
        tiered_organic_soil_parquet,
    )


@pytest.mark.asyncio
async def test_global_vegetation_sums_country_tier_only(
    tiered_analyzer, global_analysis
):
    await tiered_analyzer.analyze(global_analysis)
    df = pd.DataFrame(global_analysis.result["vegetation"])

    # One world row per land_state_class x year, not one row per country.
    assert len(df) == 1
    assert df.aoi_id.tolist() == ["GLOBAL"]
    assert df.aoi_type.tolist() == ["global"]
    # 150, not the 300 an unfiltered sum over all three tiers would give.
    assert df.gross_emissions_MgCO2e.iloc[0] == EXPECTED_GLOBAL_EMISSIONS
    assert df.area_ha.iloc[0] == 12.0


@pytest.mark.asyncio
async def test_global_agriculture_repeats_one_world_total_per_year(
    tiered_analyzer, global_analysis
):
    await tiered_analyzer.analyze(global_analysis)
    df = pd.DataFrame(global_analysis.result["agriculture"])

    # One row per category x broadcast year, and every year repeats the same
    # world total -- the snapshot is summed before the broadcast, never after.
    assert len(df) == 2 * len(ANNUALIZED_YEARS)
    for category in ("cropland", "livestock"):
        rows = df[df.category == category]
        assert set(rows.year) == set(ANNUALIZED_YEARS)
        assert (rows.gross_emissions_MgCO2e == EXPECTED_GLOBAL_EMISSIONS).all()
    assert set(df.aoi_id) == {"GLOBAL"}
    assert set(df.aoi_type) == {"global"}


@pytest.mark.asyncio
async def test_global_mineral_soil_repeats_one_world_total_per_year(
    tiered_analyzer, global_analysis
):
    await tiered_analyzer.analyze(global_analysis)
    df = pd.DataFrame(global_analysis.result["mineral_soil"])

    assert len(df) == len(ANNUALIZED_YEARS)
    assert set(df.year) == set(ANNUALIZED_YEARS)
    assert (df.gross_emissions_MgCO2e == EXPECTED_GLOBAL_EMISSIONS).all()
    assert set(df.aoi_id) == {"GLOBAL"}


@pytest.mark.asyncio
async def test_global_organic_soil_sums_each_block_separately(
    tiered_analyzer, global_analysis
):
    await tiered_analyzer.analyze(global_analysis)
    df = pd.DataFrame(global_analysis.result["organic_soil"])

    # Each 5-year block keeps its own world total across the years it covers.
    assert len(df) == len(ANNUALIZED_YEARS)
    assert (df[df.year <= 2020].gross_emissions_MgCO2e == 150.0).all()
    assert (df[df.year >= 2021].gross_emissions_MgCO2e == 300.0).all()
    assert set(df.aoi_id) == {"GLOBAL"}


@pytest.mark.asyncio
async def test_global_result_has_a_table_per_category(tiered_analyzer, global_analysis):
    await tiered_analyzer.analyze(global_analysis)

    assert set(global_analysis.result) == {
        "vegetation",
        "agriculture",
        "mineral_soil",
        "organic_soil",
    }


def test_global_query_filters_to_the_country_tier():
    query = global_query(("category",), ("gross_emissions_MgCO2e",))

    assert "not like '%.%'" in query
    assert "sum(gross_emissions_MgCO2e)" in query
    assert "group by category" in query


@pytest.mark.parametrize(
    "query",
    [
        global_query(("category",), ("gross_emissions_MgCO2e",)),
        global_query((), ("area_ha",)),
        admin_query(("category",), ("area_ha",), ["BRA"]),
    ],
)
def test_query_names_the_table_placeholder_exactly_once(query):
    # DuckDbPrecalcQueryService swaps the URI in with a plain str.replace over
    # the whole query, so any second occurrence -- a CTE named data_source_adm0,
    # say -- would be rewritten into a string literal and break the SQL.
    assert query.count("data_source") == 1
