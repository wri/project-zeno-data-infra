from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from app.domain.analyzers.tree_cover_gain_analyzer import TreeCoverGainAnalyzer
from app.domain.compute_engines.compute_engine import ComputeEngine
from app.domain.compute_engines.handlers.precalc_implementations.precalc_handlers import (
    TreeCoverGainPrecalcHandler,
)
from app.domain.compute_engines.handlers.precalc_implementations.precalc_sql_query_builder import (
    PrecalcSqlQueryBuilder,
)
from app.domain.models.analysis import Analysis
from app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.land_change.tree_cover_gain import TreeCoverGainAnalyticsIn


class TestBuildYearsHelper:
    @pytest_asyncio.fixture
    def setup_tcga(self):
        tcga = TreeCoverGainAnalyzer(compute_engine=MagicMock(spec=ComputeEngine))
        yield tcga

    def test_build_years_one_period(self, setup_tcga):
        tcga = setup_tcga
        expected = ("2000-2005",)
        actual = tcga._build_years(start_year="2000", end_year="2005")
        assert actual == expected

    def test_build_years_multiple_periods(self, setup_tcga):
        tcga = setup_tcga
        expected = ("2000-2005", "2005-2010")
        actual = tcga._build_years(start_year="2000", end_year="2010")
        assert actual == expected


class TestTreeCoverGainAnalyzerAdminAOIs:
    @pytest.mark.asyncio
    async def test_analyzer_creates_a_query_with_one_year_range(self):
        mock_query_service = MagicMock(spec=DuckDbPrecalcQueryService)
        compute_engine = ComputeEngine(
            handler=TreeCoverGainPrecalcHandler(
                precalc_query_builder=PrecalcSqlQueryBuilder(),
                precalc_query_service=mock_query_service,
                next_handler=None,
            )
        )

        analyzer = TreeCoverGainAnalyzer(compute_engine=compute_engine)
        metadata = TreeCoverGainAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.12.1"]),
            start_year="2000",
            end_year="2005",
        )
        analysis = Analysis(
            result=None, metadata=metadata.model_dump(), status=AnalysisStatus.pending
        )

        await analyzer.analyze(analysis)

        mock_query_service.execute.assert_called_once()
        args, _ = mock_query_service.execute.call_args
        sql_query = args[0]
        # Assert that the SQL query contains the multiple gain_period filters
        assert "tree_cover_gain_period in ('2000-2005')" in sql_query

        mock_query_service.reset_mock()

    @pytest.mark.asyncio
    async def test_analyzer_creates_a_query_with_multiple_year_ranges(self):
        mock_query_service = MagicMock(spec=DuckDbPrecalcQueryService)
        compute_engine = ComputeEngine(
            handler=TreeCoverGainPrecalcHandler(
                precalc_query_builder=PrecalcSqlQueryBuilder(),
                precalc_query_service=mock_query_service,
                next_handler=None,
            )
        )

        analyzer = TreeCoverGainAnalyzer(compute_engine=compute_engine)
        metadata = TreeCoverGainAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.12.1"]),
            start_year="2000",
            end_year="2010",
        )
        analysis = Analysis(
            result=None, metadata=metadata.model_dump(), status=AnalysisStatus.pending
        )

        await analyzer.analyze(analysis)

        mock_query_service.execute.assert_called_once()
        args, _ = mock_query_service.execute.call_args
        sql_query = args[0]
        # Assert that the SQL query contains the multiple gain_period filters
        assert "tree_cover_gain_period in ('2000-2005', '2005-2010')" in sql_query

        mock_query_service.reset_mock()

    @pytest.mark.asyncio
    @pytest.mark.xfail
    async def test_analyzer_adds_intact_forest_when_filtered(self):
        mock_query_service = MagicMock(spec=DuckDbPrecalcQueryService)
        compute_engine = ComputeEngine(
            handler=TreeCoverGainPrecalcHandler(
                precalc_query_builder=PrecalcSqlQueryBuilder(),
                precalc_query_service=mock_query_service,
                next_handler=None,
            )
        )

        analyzer = TreeCoverGainAnalyzer(compute_engine=compute_engine)
        metadata = TreeCoverGainAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.12.1"]),
            start_year="2000",
            end_year="2005",
            forest_filter="intact_forest",
        )
        analysis = Analysis(
            result=None, metadata=metadata.model_dump(), status=AnalysisStatus.pending
        )

        await analyzer.analyze(analysis)

        mock_query_service.execute.assert_called_once()
        args, _ = mock_query_service.execute.call_args
        sql_query = args[0]

        assert "AND is_intact_forest = True" in sql_query

        mock_query_service.reset_mock()

    @pytest.mark.asyncio
    async def test_analyzer_adds_primary_forest_when_filtered(self):
        mock_query_service = MagicMock(spec=DuckDbPrecalcQueryService)
        compute_engine = ComputeEngine(
            handler=TreeCoverGainPrecalcHandler(
                precalc_query_builder=PrecalcSqlQueryBuilder(),
                precalc_query_service=mock_query_service,
                next_handler=None,
            )
        )

        analyzer = TreeCoverGainAnalyzer(compute_engine=compute_engine)
        metadata = TreeCoverGainAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.12.1"]),
            start_year="2000",
            end_year="2005",
            forest_filter="primary_forest",
        )
        analysis = Analysis(
            result=None, metadata=metadata.model_dump(), status=AnalysisStatus.pending
        )

        await analyzer.analyze(analysis)

        mock_query_service.execute.assert_called_once()
        args, _ = mock_query_service.execute.call_args
        sql_query = args[0]

        assert "AND is_primary_forest = True" in sql_query

        mock_query_service.reset_mock()

    @pytest.mark.asyncio
    async def test_analyzer_does_NOT_add_forest_filter_when_excluded(self):
        mock_query_service = MagicMock(spec=DuckDbPrecalcQueryService)
        compute_engine = ComputeEngine(
            handler=TreeCoverGainPrecalcHandler(
                precalc_query_builder=PrecalcSqlQueryBuilder(),
                precalc_query_service=mock_query_service,
                next_handler=None,
            )
        )

        analyzer = TreeCoverGainAnalyzer(compute_engine=compute_engine)
        metadata = TreeCoverGainAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.12.1"]),
            start_year="2000",
            end_year="2005",
        )
        analysis = Analysis(
            result=None, metadata=metadata.model_dump(), status=AnalysisStatus.pending
        )

        await analyzer.analyze(analysis)

        mock_query_service.execute.assert_called_once()
        args, _ = mock_query_service.execute.call_args
        sql_query = args[0]

        assert "AND is_intact_forest = True" not in sql_query
        assert "AND is_primary_forest = True" not in sql_query

        mock_query_service.reset_mock()
