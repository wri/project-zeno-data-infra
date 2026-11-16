from dataclasses import dataclass
from typing import Any, Protocol

FINISHED_STATUSES = ("saved", "failed")


class AnalyticsRequestError(Exception):
    pass


@dataclass(frozen=True)
class AnalysisState:
    link: str
    status: str
    message: str
    retry_after_seconds: float | None
    result: dict[str, Any] | None
    metadata: dict[str, Any] | None

    @property
    def is_finished(self) -> bool:
        return self.status in FINISHED_STATUSES


@dataclass(frozen=True)
class AreaMatch:
    """An area found by name, with the area of interest to request analyses
    for it."""

    name: str
    display_name: str
    aoi: dict[str, Any]


class AnalyticsClient(Protocol):
    async def openapi_document(self) -> dict[str, Any]: ...

    async def search_areas(
        self, name: str, area_types: list[str] | None, limit: int
    ) -> list[AreaMatch]: ...

    async def create_analysis(self, path: str, payload: dict[str, Any]) -> str:
        """Start an analysis and return the link to poll for its state."""
        ...

    async def get_analysis(self, link: str) -> AnalysisState: ...
