from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn
from app.models.common.analysis import AnalysisStatus


class TreeCoverLossService:
    def __init__(self, utils=None):
        pass

    def do(self, tree_cover_loss_analytics: TreeCoverLossAnalyticsIn):
        pass

    def get_status(self):
        return AnalysisStatus.pending
