from abc import ABC, abstractmethod

from app.domain.models.analysis import Analysis


class Analyzer(ABC):
    def __init__(
        self,
        analysis_repository=None,
        compute_engine=None,
        dataset_repository=None,
    ):
        self.analysis_repository = analysis_repository
        self.compute_engine = compute_engine
        self.dataset_repository = dataset_repository

    @abstractmethod
    def analyze(self, analysis: Analysis):
        pass
