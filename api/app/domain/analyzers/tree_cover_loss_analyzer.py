from app.domain.analyzers.analyzer import Analyzer
from app.domain.models.analysis import Analysis


class TreeCoverLossAnalyzer(Analyzer):
    def __init__(self, analysis_repository=None, otf_compute_engine=None, dataset_repository=None):
        self.analysis_repository = analysis_repository  # TreeCoverLossRepository
        self.otf_compute_engine = otf_compute_engine  # Dask Client
        self.dataset_repository = dataset_repository  # AWS-S3 for zarrs, etc.


    def analyze(self, analysis: Analysis):
        pass
