from abc import ABC, abstractmethod

from app.domain.models.analysis import Analysis


class Analyzer(ABC):
    @abstractmethod
    async def analyze(self, analysis: Analysis):
        pass
