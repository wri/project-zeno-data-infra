import pytest
from app.domain.analyzers.tree_cover_loss_analyzer import TreeCoverLossAnalyzer
from app.domain.models.analysis import Analysis
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn


class DummyAnalysisRepository:
    def store_analysis(self, resource_id, analysis):
        self.analysis = analysis


class DummyComputeEngine:
    def send_query(self, dataset, version, query):
        self.dataset = dataset
        self.version = version
        self.query = query


@pytest.mark.asyncio
async def test_tree_cover_loss_analysis():
    analysis_repo = DummyAnalysisRepository()
    compute_engine = DummyComputeEngine()

    analyzer = TreeCoverLossAnalyzer(
        analysis_repository=analysis_repo, compute_engine=compute_engine
    )
    metadata = TreeCoverLossAnalyticsIn(
        aoi={"type": "admin", "ids": ["BRA.12.1"]},
        start_year="2020",
        end_year="2023",
        canopy_cover=30,
        forest_filter="intact_forest",
        intersections=["driver"],  # Replace with actual enum
    ).model_dump()

    analysis = Analysis(None, metadata, AnalysisStatus.saved)
    await analyzer.analyze(analysis)

    assert compute_engine.dataset == "gadm__tcl__iso_change"
