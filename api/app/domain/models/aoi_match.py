from dataclasses import dataclass


@dataclass(frozen=True)
class AoiMatch:
    """An AOI whose name matches a search."""

    aoi_type: str
    aoi_id: str
    name: str
    display_name: str
