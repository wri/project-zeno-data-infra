from typing import Literal, Sequence

from app.domain.repositories.aoi_geometry_repository import Bounds

DeforestationForestFilter = Literal["primary_forest", "intact_forest"]


class DeforestationForest:
    """Chooses the forest deforestation is measured in: humid tropical primary
    forest when every area is centred within the tropics, where that map
    exists, and intact forest landscapes, which cover the globe, otherwise."""

    def __init__(self, tropics_latitude: float):
        self.tropics_latitude = tropics_latitude

    def forest_filter(self, bounds: Sequence[Bounds]) -> DeforestationForestFilter:
        if bounds and all(self.in_tropics(area) for area in bounds):
            return "primary_forest"
        return "intact_forest"

    def in_tropics(self, area: Bounds) -> bool:
        _, min_latitude, _, max_latitude = area
        return abs((min_latitude + max_latitude) / 2) <= self.tropics_latitude
