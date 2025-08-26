from unittest.mock import MagicMock

import pytest
from app.domain.analyzers.tree_cover_gain_analyzer import TreeCoverGainAnalyzer
from app.domain.compute_engines.compute_engine import ComputeEngine
from app.domain.models.analysis import Analysis
from app.domain.models.dataset import (
    Dataset,
    DatasetAggregate,
    DatasetFilter,
    DatasetQuery,
)
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import AdminAreaOfInterest
from app.models.land_change.tree_cover_gain import TreeCoverGainAnalyticsIn


class TestTreeCoverGainAnalyzer:
    @pytest.mark.asyncio
    async def test_happy_path(self):
        mock_compute_engine = MagicMock(spec=ComputeEngine)
        analyzer = TreeCoverGainAnalyzer(compute_engine=mock_compute_engine)
        metadata = TreeCoverGainAnalyticsIn(
            aoi=AdminAreaOfInterest(ids=["BRA.12.1"]),
            start_year="2000",
            end_year="2005",
        )
        analysis = Analysis(
            result=None, metadata=metadata.model_dump(), status=AnalysisStatus.pending
        )

        await analyzer.analyze(analysis)

        mock_compute_engine.compute.assert_called_once_with(
            metadata.aoi.type,
            metadata.aoi.ids,
            DatasetQuery(
                aggregate=DatasetAggregate(dataset=Dataset.area_hectares, func="sum"),
                group_bys=[Dataset.tree_cover_gain],
                filters=[
                    DatasetFilter(
                        dataset=Dataset.tree_cover_gain,
                        op=">=",
                        value="2000",
                    ),
                    DatasetFilter(
                        dataset=Dataset.tree_cover_gain,
                        op="<=",
                        value="2005",
                    ),
                ],
            ),
        )

        mock_compute_engine.reset_mock()
