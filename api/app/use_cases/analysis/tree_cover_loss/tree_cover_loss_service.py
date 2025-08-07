from uuid import UUID

from app.models.common.analysis import AnalysisStatus
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn, TreeCoverLossAnalytics
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository


class TreeCoverLossService:
    def __init__(self, analysis_repository: AnalysisRepository):
        self.analytics_resource: TreeCoverLossAnalytics = None
        self.analysis_repository = analysis_repository

    def do(self) -> None:
        if self.analytics_resource.status == AnalysisStatus.saved: return

        pass

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
    