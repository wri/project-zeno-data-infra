import pytest
import pytest_asyncio
from app.domain.analyzers.tree_cover_loss_analyzer import TreeCoverLossAnalyzer
from app.domain.models.analysis import Analysis
from app.infrastructure.external_services.compute_service import ComputeService
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn


class DummyAnalysisRepository:
    def __init__(self):
        self.analysis = None

    async def store_analysis(self, resource_id, analysis):
        self.analysis = analysis


class DummyComputeEngine(ComputeService):
    def __init__(self, result):
        self.dataset = None
        self.version = None
        self.query = None
        self.result = result

    async def compute(self, payload: dict):
        self.dataset = payload["dataset"]
        self.version = payload["version"]
        self.query = payload["query"]

        return self.result


class TestTreeCoverLossOneIso:
    @pytest_asyncio.fixture(autouse=True)
    async def run_analysis(self):
        dummy_result = [
            {
                "iso": "BRA",
                "year": 2020,
                "tree_cover_loss_ha": 10,
                "carbon_emissions_Mg": 100,
            },
            {
                "iso": "BRA",
                "year": 2023,
                "tree_cover_loss_ha": 100,
                "carbon_emissions_Mg": 1000,
            },
        ]

        self.analysis_repo = DummyAnalysisRepository()
        self.compute_engine = DummyComputeEngine(dummy_result)

        analyzer = TreeCoverLossAnalyzer(
            analysis_repository=self.analysis_repo, compute_engine=self.compute_engine
        )
        self.metadata = TreeCoverLossAnalyticsIn(
            aoi={"type": "admin", "ids": ["BRA"]},
            start_year="2020",
            end_year="2023",
            canopy_cover=30,
            forest_filter="intact_forest",
            intersections=[],
        ).model_dump()

        analysis = Analysis(None, self.metadata, AnalysisStatus.saved)
        await analyzer.analyze(analysis)

    @pytest.mark.asyncio
    async def test_query(self):
        assert self.compute_engine.dataset == "gadm__tcl__iso_change"
        assert self.compute_engine.version == "v20250515"
        assert self.compute_engine.query == (
            "SELECT iso, umd_tree_cover_loss__year AS year, SUM(umd_tree_cover_loss__ha) AS tree_cover_loss_ha, "
            'SUM("gfw_gross_emissions_co2e_all_gases__Mg") AS '
            '"carbon_emissions_Mg" FROM data WHERE '
            "umd_tree_cover_density_2000__threshold = 30 AND iso in ('BRA') AND umd_tree_cover_loss__year >= 2020 AND umd_tree_cover_loss__year <= 2023 AND "
            "is__ifl_intact_forest_landscapes_2000 = true GROUP BY iso, "
            "umd_tree_cover_loss__year"
        )

    @pytest.mark.asyncio
    async def test_analysis_result(self):
        assert self.analysis_repo.analysis is not None
        assert self.analysis_repo.analysis.status == AnalysisStatus.saved
        assert self.analysis_repo.analysis.metadata == self.metadata
        assert self.analysis_repo.analysis.result == {
            "id": ["BRA", "BRA"],
            "year": [2020, 2023],
            "tree_cover_loss_ha": [10, 100],
            "carbon_emissions_Mg": [100, 1000],
        }


class TestTreeCoverLossMultipleIsos:
    @pytest_asyncio.fixture(autouse=True)
    async def run_analysis(self):
        dummy_result = [
            {
                "iso": "BRA",
                "year": 2020,
                "tree_cover_loss_ha": 10,
                "carbon_emissions_Mg": 100,
            },
            {
                "iso": "BRA",
                "year": 2023,
                "tree_cover_loss_ha": 100,
                "carbon_emissions_Mg": 1000,
            },
            {
                "iso": "IDN",
                "year": 2020,
                "tree_cover_loss_ha": 11,
                "carbon_emissions_Mg": 110,
            },
            {
                "iso": "IDN",
                "year": 2023,
                "tree_cover_loss_ha": 110,
                "carbon_emissions_Mg": 1100,
            },
            {
                "iso": "COD",
                "year": 2020,
                "tree_cover_loss_ha": 12,
                "carbon_emissions_Mg": 120,
            },
            {
                "iso": "COD",
                "year": 2023,
                "tree_cover_loss_ha": 120,
                "carbon_emissions_Mg": 1200,
            },
        ]

        self.analysis_repo = DummyAnalysisRepository()
        self.compute_engine = DummyComputeEngine(dummy_result)

        analyzer = TreeCoverLossAnalyzer(
            analysis_repository=self.analysis_repo, compute_engine=self.compute_engine
        )
        self.metadata = TreeCoverLossAnalyticsIn(
            aoi={"type": "admin", "ids": ["BRA", "IDN", "COD"]},
            start_year="2020",
            end_year="2023",
            canopy_cover=30,
            forest_filter="intact_forest",
            intersections=[],
        ).model_dump()

        analysis = Analysis(None, self.metadata, AnalysisStatus.saved)
        await analyzer.analyze(analysis)

    @pytest.mark.asyncio
    async def test_query(self):
        assert self.compute_engine.dataset == "gadm__tcl__iso_change"
        assert self.compute_engine.version == "v20250515"
        assert self.compute_engine.query == (
            "SELECT iso, umd_tree_cover_loss__year AS year, SUM(umd_tree_cover_loss__ha) AS tree_cover_loss_ha, "
            'SUM("gfw_gross_emissions_co2e_all_gases__Mg") AS "carbon_emissions_Mg"'
            " FROM data WHERE "
            "umd_tree_cover_density_2000__threshold = 30 AND iso in ('BRA', 'IDN', 'COD') AND umd_tree_cover_loss__year >= 2020 AND umd_tree_cover_loss__year <= 2023 AND "
            "is__ifl_intact_forest_landscapes_2000 = true GROUP BY iso, "
            "umd_tree_cover_loss__year"
        )

    @pytest.mark.asyncio
    async def test_analysis_result(self):
        assert self.analysis_repo.analysis is not None
        assert self.analysis_repo.analysis.status == AnalysisStatus.saved
        assert self.analysis_repo.analysis.metadata == self.metadata
        assert self.analysis_repo.analysis.result == {
            "id": ["BRA", "BRA", "IDN", "IDN", "COD", "COD"],
            "year": [2020, 2023] * 3,
            "tree_cover_loss_ha": [10, 100, 11, 110, 12, 120],
            "carbon_emissions_Mg": [100, 1000, 110, 1100, 120, 1200],
        }


class TestTreeCoverLossMultipleAdm1s:
    @pytest_asyncio.fixture(autouse=True)
    async def run_analysis(self):
        dummy_result = [
            {
                "iso": "BRA",
                "adm1": 1,
                "year": 2020,
                "tree_cover_loss_ha": 10,
                "carbon_emissions_Mg": 100,
            },
            {
                "iso": "BRA",
                "adm1": 1,
                "year": 2023,
                "tree_cover_loss_ha": 100,
                "carbon_emissions_Mg": 1000,
            },
            {
                "iso": "IDN",
                "adm1": 24,
                "year": 2020,
                "tree_cover_loss_ha": 11,
                "carbon_emissions_Mg": 110,
            },
            {
                "iso": "IDN",
                "adm1": 24,
                "year": 2023,
                "tree_cover_loss_ha": 110,
                "carbon_emissions_Mg": 1100,
            },
            {
                "iso": "COD",
                "adm1": 5,
                "year": 2020,
                "tree_cover_loss_ha": 12,
                "carbon_emissions_Mg": 120,
            },
            {
                "iso": "COD",
                "adm1": 5,
                "year": 2023,
                "tree_cover_loss_ha": 120,
                "carbon_emissions_Mg": 1200,
            },
        ]

        self.analysis_repo = DummyAnalysisRepository()
        self.compute_engine = DummyComputeEngine(dummy_result)

        analyzer = TreeCoverLossAnalyzer(
            analysis_repository=self.analysis_repo, compute_engine=self.compute_engine
        )
        self.metadata = TreeCoverLossAnalyticsIn(
            aoi={"type": "admin", "ids": ["BRA.1", "IDN.24", "COD.5"]},
            start_year="2020",
            end_year="2023",
            canopy_cover=30,
            forest_filter="intact_forest",
            intersections=[],
        ).model_dump()

        analysis = Analysis(None, self.metadata, AnalysisStatus.saved)
        await analyzer.analyze(analysis)

    @pytest.mark.asyncio
    async def test_query(self):
        assert self.compute_engine.dataset == "gadm__tcl__adm1_change"
        assert self.compute_engine.version == "v20250515"
        assert self.compute_engine.query == (
            "SELECT iso, adm1, umd_tree_cover_loss__year AS year, SUM(umd_tree_cover_loss__ha) AS tree_cover_loss_ha, "
            'SUM("gfw_gross_emissions_co2e_all_gases__Mg") AS "carbon_emissions_Mg"'
            " FROM data WHERE "
            "umd_tree_cover_density_2000__threshold = 30 AND (iso, adm1) in (('BRA', 1), ('IDN', 24), ('COD', 5)) "
            "AND umd_tree_cover_loss__year >= 2020 AND umd_tree_cover_loss__year <= 2023 AND "
            "is__ifl_intact_forest_landscapes_2000 = true GROUP BY iso, adm1, "
            "umd_tree_cover_loss__year"
        )

    @pytest.mark.asyncio
    async def test_analysis_result(self):
        assert self.analysis_repo.analysis is not None
        assert self.analysis_repo.analysis.status == AnalysisStatus.saved
        assert self.analysis_repo.analysis.metadata == self.metadata
        assert self.analysis_repo.analysis.result == {
            "id": ["BRA.1", "BRA.1", "IDN.24", "IDN.24", "COD.5", "COD.5"],
            "year": [2020, 2023] * 3,
            "tree_cover_loss_ha": [10, 100, 11, 110, 12, 120],
            "carbon_emissions_Mg": [100, 1000, 110, 1100, 120, 1200],
        }


class TestTreeCoverLossMultipleAdm2s:
    @pytest_asyncio.fixture(autouse=True)
    async def run_analysis(self):
        dummy_result = [
            {
                "iso": "BRA",
                "adm1": 1,
                "adm2": 1,
                "year": 2020,
                "tree_cover_loss_ha": 10,
                "carbon_emissions_Mg": 100,
            },
            {
                "iso": "BRA",
                "adm1": 1,
                "adm2": 1,
                "year": 2023,
                "tree_cover_loss_ha": 100,
                "carbon_emissions_Mg": 1000,
            },
            {
                "iso": "IDN",
                "adm1": 24,
                "adm2": 9,
                "year": 2020,
                "tree_cover_loss_ha": 11,
                "carbon_emissions_Mg": 110,
            },
            {
                "iso": "IDN",
                "adm1": 24,
                "adm2": 9,
                "year": 2023,
                "tree_cover_loss_ha": 110,
                "carbon_emissions_Mg": 1100,
            },
            {
                "iso": "COD",
                "adm1": 5,
                "adm2": 1,
                "year": 2020,
                "tree_cover_loss_ha": 12,
                "carbon_emissions_Mg": 120,
            },
            {
                "iso": "COD",
                "adm1": 5,
                "adm2": 1,
                "year": 2023,
                "tree_cover_loss_ha": 120,
                "carbon_emissions_Mg": 1200,
            },
        ]

        self.analysis_repo = DummyAnalysisRepository()
        self.compute_engine = DummyComputeEngine(dummy_result)

        analyzer = TreeCoverLossAnalyzer(
            analysis_repository=self.analysis_repo, compute_engine=self.compute_engine
        )
        self.metadata = TreeCoverLossAnalyticsIn(
            aoi={"type": "admin", "ids": ["BRA.1.1", "IDN.24.9", "COD.5.1"]},
            start_year="2020",
            end_year="2023",
            canopy_cover=30,
            forest_filter="intact_forest",
            intersections=[],
        ).model_dump()

        analysis = Analysis(None, self.metadata, AnalysisStatus.saved)
        await analyzer.analyze(analysis)

    @pytest.mark.asyncio
    async def test_query(self):
        assert self.compute_engine.dataset == "gadm__tcl__adm2_change"
        assert self.compute_engine.version == "v20250515"
        assert self.compute_engine.query == (
            "SELECT iso, adm1, adm2, umd_tree_cover_loss__year AS year, SUM(umd_tree_cover_loss__ha) AS tree_cover_loss_ha, "
            'SUM("gfw_gross_emissions_co2e_all_gases__Mg") AS "carbon_emissions_Mg"'
            " FROM data WHERE "
            "umd_tree_cover_density_2000__threshold = 30 AND (iso, adm1, adm2) in (('BRA', 1, 1), ('IDN', 24, 9), ('COD', 5, 1)) "
            "AND umd_tree_cover_loss__year >= 2020 AND umd_tree_cover_loss__year <= 2023 AND "
            "is__ifl_intact_forest_landscapes_2000 = true GROUP BY iso, adm1, adm2, "
            "umd_tree_cover_loss__year"
        )

    @pytest.mark.asyncio
    async def test_analysis_result(self):
        assert self.analysis_repo.analysis is not None
        assert self.analysis_repo.analysis.status == AnalysisStatus.saved
        assert self.analysis_repo.analysis.metadata == self.metadata
        assert self.analysis_repo.analysis.result == {
            "id": ["BRA.1.1", "BRA.1.1", "IDN.24.9", "IDN.24.9", "COD.5.1", "COD.5.1"],
            "year": [2020, 2023] * 3,
            "tree_cover_loss_ha": [10, 100, 11, 110, 12, 120],
            "carbon_emissions_Mg": [100, 1000, 110, 1100, 120, 1200],
        }
