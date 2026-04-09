import uuid
from abc import ABC, abstractmethod

from app.domain.models.analysis import Analysis


class Analyzer(ABC):
    @abstractmethod
    async def analyze(self, analysis: Analysis) -> None:
        pass

    def thumbprint(self) -> uuid.UUID:
        # FIXME: Make this method abstract once all Analyzers implement it
        return uuid.uuid5(uuid.NAMESPACE_DNS, "dummy")
