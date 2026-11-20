from typing import List

from shapely.geometry import shape

from app.domain.repositories.aoi_geometry_repository import (
    AoiGeometryRepository,
    Bounds,
)
from app.domain.services.deforestation_forest import DeforestationForest
from app.models.land_change.deforestation import DeforestationAnalyticsIn
from app.models.land_change.tree_cover_loss import TreeCoverLossAnalyticsIn


class DeforestationRequest:
    """Turns a deforestation request into tree cover loss in the forest the
    area's location calls for."""

    def __init__(self, geometries: AoiGeometryRepository, forest: DeforestationForest):
        self.geometries = geometries
        self.forest = forest

    async def tree_cover_loss_request(
        self, request: DeforestationAnalyticsIn
    ) -> TreeCoverLossAnalyticsIn:
        bounds = await self.bounds(request.aoi)
        return request.tree_cover_loss_request(self.forest.forest_filter(bounds))

    async def bounds(self, aoi) -> List[Bounds]:
        """Polygons sent in the request are used as they are; other areas are
        looked up."""
        if aoi.type == "feature_collection":
            features = aoi.feature_collection["features"]
            return [shape(feature["geometry"]).bounds for feature in features]
        return await self.geometries.bounds(aoi)
