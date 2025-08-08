from app.models.common.analysis import AnalysisStatus
from app.models.land_change.tree_cover import TreeCoverAnalyticsIn


class TreeCoverService:
    def __init__(self, utils=None):
        pass

    def do(self, tree_cover_analytics: TreeCoverAnalyticsIn):
        pass

    def get_status(self):
        return AnalysisStatus.pending
