from abc import ABC, abstractmethod

from app.domain.models.analysis import Analysis


class Analyzer(ABC):
    @abstractmethod
    async def analyze(self, analysis: Analysis) -> None:
        pass

    # @abstractmethod
    # def input_uris(self) -> list[str]:
    #     """Return the sorted URIs of all data sources this analyzer reads.
    #
    #     The returned list is used to build a fingerprint for cache
    #     invalidation: when any URI changes (e.g. a dataset is updated to a
    #     new version) cached results that depended on the old URI are
    #     automatically invalidated.
    #
    #     Implementations must return a *sorted* list so that the fingerprint
    #     is deterministic.
    #     """
    #     pass
