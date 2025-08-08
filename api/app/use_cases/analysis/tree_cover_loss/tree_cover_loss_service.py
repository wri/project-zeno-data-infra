import logging
from uuid import UUID
import traceback

from app.models.common.analysis import AnalysisStatus
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn, TreeCoverLossAnalytics
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.domain.analyzers.tree_cover_loss_analyzer import TreeCoverLossAnalyzer


class TreeCoverLossService:
    def __init__(
        self,
        analysis_repository: AnalysisRepository,
        analyzer: TreeCoverLossAnalyzer
    ):
        self.analytics_resource: TreeCoverLossAnalytics = None
        self.analysis_repository = analysis_repository
        self.analyzer = analyzer

    async def do(self) -> None:
        try:
            if self.analytics_resource.status == AnalysisStatus.saved:
                return

            await self.analyzer.analyze(
                Analysis(
                    metadata=self.analytics_resource.metadata,
                    result=self.analytics_resource.result,
                    status=self.analytics_resource.status,
                )
            )

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
        return self.analytics_resource.status

    async def set_resource_from(self, data: TreeCoverLossAnalyticsIn):
        analysis: Analysis = await self.analysis_repository.load_analysis(data.thumbprint())
        status = AnalysisStatus.pending

        if analysis.status == AnalysisStatus.failed:
            raise Exception('Analysis failed')

        if analysis.result is not None:
            status = AnalysisStatus.saved

        self.analytics_resource = TreeCoverLossAnalytics(
            metadata=data.model_dump(),
            result=analysis.result,
            status=status
        )

        await self.analysis_repository.store_analysis(data.thumbprint(), analysis)

    def resource_thumbprint(self) -> UUID:
        return TreeCoverLossAnalyticsIn(**self.analytics_resource.metadata).thumbprint()
    