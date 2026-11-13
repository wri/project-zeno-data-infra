from shapely.geometry.base import BaseGeometry

from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository
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
        geometries: list[BaseGeometry] = []
        if not request.aoi.covers_globe():
            geometries, _ = await self.geometries.load(request.aoi)
        return request.tree_cover_loss_request(self.forest.forest_filter(geometries))
