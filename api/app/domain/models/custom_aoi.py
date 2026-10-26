from dataclasses import dataclass
from enum import Enum

from shapely.geometry import MultiPolygon


class MonitorStatus(str, Enum):
    monitored = "monitored"
    not_monitored = "not_monitored"


@dataclass(frozen=True)
class CustomAoi:
    aoi_id: str
    geometry: MultiPolygon
    area_ha: float
    name: str
    user_id: str
    email: str
    monitor_status: MonitorStatus


@dataclass(frozen=True)
class CustomAoiGeometry:
    """A stored custom AOI's geometry, without its owner's details."""

    aoi_id: str
    geometry: MultiPolygon
