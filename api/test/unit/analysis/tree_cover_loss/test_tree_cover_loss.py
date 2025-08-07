import pytest
from app.domain.analyzers.tree_cover_loss_analyzer import TreeCoverLossAnalyzer
from app.domain.models.analysis import Analysis
from app.infrastructure.external_services.compute_service import ComputeService
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn


class DummyAnalysisRepository:
    async def store_analysis(self, resource_id, analysis):
        self.analysis = analysis


class DummyComputeEngine(ComputeService):
    def __init__(self):
        self.dataset = None
        self.version = None
        self.query = None

    async def compute(self, payload: dict):
        self.dataset = payload["dataset"]
        self.version = payload["version"]
        self.query = payload["query"]

        return [
            {
                "iso": "BRA",
                "year": 2000,
                "umd_tree_cover_loss__year": 10,
                "gfw_gross_emissions_co2e_all_gases__Mg": 100,
            },
            {
                "iso": "BRA",
                "year": 2024,
                "umd_tree_cover_loss__year": 100,
                "gfw_gross_emissions_co2e_all_gases__Mg": 1000,
            },
        ]


@pytest.mark.asyncio
async def test_tree_cover_loss_analysis_one_iso():
    analysis_repo = DummyAnalysisRepository()
    compute_engine = DummyComputeEngine()

    analyzer = TreeCoverLossAnalyzer(
        analysis_repository=analysis_repo, compute_engine=compute_engine
    )
    metadata = TreeCoverLossAnalyticsIn(
        aoi={"type": "admin", "ids": ["BRA"]},
        start_year="2020",
        end_year="2023",
        canopy_cover=30,
        forest_filter="intact_forest",
        intersections=["driver"],  # Replace with actual enum
    ).model_dump()

    analysis = Analysis(None, metadata, AnalysisStatus.saved)
    await analyzer.analyze(analysis)

    assert compute_engine.dataset == "gadm__tcl__iso_change"
    assert compute_engine.version == "v20250515"
    assert compute_engine.query == (
        "SELECT iso, umd_tree_cover_loss__year, SUM(umd_tree_cover_loss__ha) AS umd_tree_cover_loss__ha, "
        'SUM("gfw_gross_emissions_co2e_all_gases__Mg") AS '
        '"gfw_gross_emissions_co2e_all_gases__Mg") FROM data WHERE '
        "umd_tree_cover_density_2000__threshold = 30 AND iso in ('BRA',) AND "
        "is__ifl_intact_forest_landscapes_2000 = true GROUP BY iso, "
        "umd_tree_cover_loss__year"
    )
