from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class Source:
    """An external dataset a pipeline reads, and where to learn more about it."""

    uri: str
    title: str
    description: str
    url: str


class SourceCatalog(ABC):
    @abstractmethod
    def source(self, source_id: str) -> Source: ...
