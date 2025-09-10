from unittest.mock import MagicMock

import pytest
from app.domain.analyzers.tree_cover_analyzer import TreeCoverAnalyzer
from app.domain.compute_engines.compute_engine import ComputeEngine
from app.domain.compute_engines.handlers.precalc_implementations.precalc_handlers import (
    TreeCoverPrecalcHandler,
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
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn


class TestTreeCoverAnalyzerAdminAOIs:
    @pytest.mark.asyncio
    async def test_analyzer_creates_a_query_with_valid_canopy_cover(self):
        mock_query_service = MagicMock(spec=DuckDbPrecalcQueryService)
        compute_engine = ComputeEngine(
            handler=TreeCoverPrecalcHandler(
                precalc_query_builder=PrecalcSqlQueryBuilder(),
                precalc_query_service=mock_query_service,
                next_handler=None,
            )
        )

        analyzer = TreeCoverAnalyzer(compute_engine=compute_engine)
        metadata = TreeCoverAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.10"]),
            canopy_cover=15,
        )
        analysis = Analysis(
            result=None, metadata=metadata.model_dump(), status=AnalysisStatus.pending
        )

        await analyzer.analyze(analysis)

        mock_query_service.execute.assert_called_once()
        args, _ = mock_query_service.execute.call_args
        sql_query = args[0]
        # Assert that the SQL query contains the canopy cover filter
        assert " canopy_cover >= 15" in sql_query

        mock_query_service.reset_mock()

    @pytest.mark.asyncio
    @pytest.mark.xfail
    async def test_analyzer_adds_forest_filter_when_specified(self):
        mock_query_service = MagicMock(spec=DuckDbPrecalcQueryService)
        compute_engine = ComputeEngine(
            handler=TreeCoverPrecalcHandler(
                precalc_query_builder=PrecalcSqlQueryBuilder(),
                precalc_query_service=mock_query_service,
                next_handler=None,
            )
        )

        analyzer = TreeCoverAnalyzer(compute_engine=compute_engine)
        metadata = TreeCoverAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.10"]),
            canopy_cover=15,
            forest_filter="intact_forest",
        )
        analysis = Analysis(
            result=None, metadata=metadata.model_dump(), status=AnalysisStatus.pending
        )

        await analyzer.analyze(analysis)

        mock_query_service.execute.assert_called_once()
        args, _ = mock_query_service.execute.call_args
        sql_query = args[0]

        assert " is_intact_forest = True" in sql_query

    @pytest.mark.asyncio
    @pytest.mark.xfail
    async def test_analyzer_adds_intact_forest_filter_when_specified(self):
        mock_query_service = MagicMock(spec=DuckDbPrecalcQueryService)
        compute_engine = ComputeEngine(
            handler=TreeCoverPrecalcHandler(
                precalc_query_builder=PrecalcSqlQueryBuilder(),
                precalc_query_service=mock_query_service,
                next_handler=None,
            )
        )

        analyzer = TreeCoverAnalyzer(compute_engine=compute_engine)
        metadata = TreeCoverAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.10"]),
            canopy_cover=15,
            forest_filter="intact_forest",
        )
        analysis = Analysis(
            result=None, metadata=metadata.model_dump(), status=AnalysisStatus.pending
        )

        await analyzer.analyze(analysis)

        mock_query_service.execute.assert_called_once()
        args, _ = mock_query_service.execute.call_args
        sql_query = args[0]

        assert " is_intact_forest = True" in sql_query

    @pytest.mark.asyncio
    async def test_analyzer_adds_primary_forest_filter_when_specified(self):
        mock_query_service = MagicMock(spec=DuckDbPrecalcQueryService)
        compute_engine = ComputeEngine(
            handler=TreeCoverPrecalcHandler(
                precalc_query_builder=PrecalcSqlQueryBuilder(),
                precalc_query_service=mock_query_service,
                next_handler=None,
            )
        )

        analyzer = TreeCoverAnalyzer(compute_engine=compute_engine)
        metadata = TreeCoverAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.10"]),
            canopy_cover=15,
            forest_filter="primary_forest",
        )
        analysis = Analysis(
            result=None, metadata=metadata.model_dump(), status=AnalysisStatus.pending
        )

        await analyzer.analyze(analysis)

        mock_query_service.execute.assert_called_once()
        args, _ = mock_query_service.execute.call_args
        sql_query = args[0]

        assert " is_primary_forest = True" in sql_query

    @pytest.mark.asyncio
    async def test_analyzer_does_not_add_forest_filter_when_not_specified(self):
        mock_query_service = MagicMock(spec=DuckDbPrecalcQueryService)
        compute_engine = ComputeEngine(
            handler=TreeCoverPrecalcHandler(
                precalc_query_builder=PrecalcSqlQueryBuilder(),
                precalc_query_service=mock_query_service,
                next_handler=None,
            )
        )

        analyzer = TreeCoverAnalyzer(compute_engine=compute_engine)
        metadata = TreeCoverAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.10"]),
            canopy_cover=15,
        )
        analysis = Analysis(
            result=None, metadata=metadata.model_dump(), status=AnalysisStatus.pending
        )

        await analyzer.analyze(analysis)

        mock_query_service.execute.assert_called_once()
        args, _ = mock_query_service.execute.call_args
        sql_query = args[0]

        assert " is_intact_forest" not in sql_query
        assert " is_primary_forest" not in sql_query
