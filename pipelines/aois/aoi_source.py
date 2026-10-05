from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AoiSource:
    """Where an AOI dataset lives and how its (lowercased) attribute columns map
    onto AOI fields. The display name joins `name_columns` in order."""

    source: str
    version: str
    uri: str
    layer: Optional[str]
    id_column: str
    subtype: str
    name_columns: tuple[str, ...]
    iso3_column: str
