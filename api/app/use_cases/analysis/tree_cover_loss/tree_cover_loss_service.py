import logging
import traceback
from uuid import UUID, uuid4

from app.domain.analyzers.tree_cover_loss_analyzer import TreeCoverLossAnalyzer
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.models.common.analysis import AnalysisStatus
from app.models.land_change.tree_cover_loss import (
    TreeCoverLossAnalytics,
    TreeCoverLossAnalyticsIn,
)


class TreeCoverLossService:
    def __init__(
        self, analysis_repository: AnalysisRepository, analyzer: TreeCoverLossAnalyzer
    ):
        self.analysis_repository = analysis_repository
        self.analyzer = analyzer
        self.analytics_resource: TreeCoverLossAnalytics = TreeCoverLossAnalytics()
        self.analytics_resource_id = uuid4()

    async def do(self) -> None:
        try:
            if self.analytics_resource.status is not None:
                return  # analysis is in progress, complete, or failed

            self.analytics_resource.status = AnalysisStatus.pending

            analysis = Analysis(
                metadata=self.analytics_resource.metadata,
                result=self.analytics_resource.result,
                status=self.analytics_resource.status,
            )

            await self.analysis_repository.store_analysis(
                self.analytics_resource_id, analysis
            )
            await self.analyzer.analyze(analysis)
        except Exception as e:
            logging.error(
                {
                    "event": "tree_cover_loss_analytics_processing_failure",
                    "severity": "high",
                    "metadata": self.analytics_resource.metadata,
                    "analysis_repository": self.analysis_repository,
                    "analyzer": self.analyzer,
                    "error_type": e.__class__.__name__,
                    "error_details": str(e),
                    "stack_trace": traceback.format_exc(),
                }
            )

    def get_status(self) -> AnalysisStatus:
        return self.analytics_resource.status or AnalysisStatus.pending

    async def set_resource_from(self, data: TreeCoverLossAnalyticsIn):
        analysis: Analysis = await self.analysis_repository.load_analysis(
            data.thumbprint()
        )
        self.analytics_resource_id = data.thumbprint()
        self.analytics_resource = TreeCoverLossAnalytics(
            metadata=analysis.metadata or data.model_dump(),
            result=analysis.result,
            status=analysis.status,
        )

    def resource_thumbprint(self) -> UUID:
        return self.analytics_resource_id
