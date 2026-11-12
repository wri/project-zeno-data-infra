from dataclasses import dataclass


@dataclass(frozen=True)
class AoiSubregion:
    """An AOI that lies within another AOI."""

    aoi_type: str
    aoi_id: str
    name: str
    area_ha: float
