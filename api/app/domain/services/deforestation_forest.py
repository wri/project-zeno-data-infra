from typing import Literal, Sequence

from shapely.geometry.base import BaseGeometry

DeforestationForestFilter = Literal["primary_forest", "intact_forest"]


class DeforestationForest:
    """Chooses the forest deforestation is measured in: humid tropical primary
    forest when every area is centred within the tropics, where that map
    exists, and intact forest landscapes, which cover the globe, otherwise."""

    def __init__(self, tropics_latitude: float):
        self.tropics_latitude = tropics_latitude

    def forest_filter(
        self, geometries: Sequence[BaseGeometry]
    ) -> DeforestationForestFilter:
        if geometries and all(self.in_tropics(area) for area in geometries):
            return "primary_forest"
        return "intact_forest"

    def in_tropics(self, area: BaseGeometry) -> bool:
        return abs(area.centroid.y) <= self.tropics_latitude
