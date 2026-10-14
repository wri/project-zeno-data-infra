from dataclasses import dataclass
from typing import Optional

from pipelines.aois.aoi_ids import AoiIds


@dataclass(frozen=True)
class AoiSource:
    """Where an AOI dataset lives, which of its rows to read (`where` is a
    filter in the dataset's own SQL dialect), and how each row's AOI id is
    derived from its (lowercased) attribute columns."""

    source: str
    version: str
    uri: str
    layer: Optional[str]
    aoi_ids: AoiIds
    where: Optional[str] = None
