from uuid import UUID

from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn
from app.models.common.analysis import AnalysisStatus


class TreeCoverLossService:
    def __init__(self):
        self.analytics_resource = None

    def do(self) -> None:
        pass

    def get_status(self) -> AnalysisStatus:
        return AnalysisStatus.pending

    def set_resource_from(self, data: TreeCoverLossAnalyticsIn):
        self.analytics_resource = data

    def resource_thumbprint(self) -> UUID:
        return self.analytics_resource.thumbprint()
    