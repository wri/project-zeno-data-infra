import pytest
from shapely.geometry import box

from app.domain.repositories.aoi_geometry_repository import AoiGeometryRepository
from app.domain.services.deforestation_forest import DeforestationForest
from app.models.land_change.deforestation import DeforestationAnalyticsIn
from app.use_cases.analysis.deforestation_request import DeforestationRequest


class FixedGeometries(AoiGeometryRepository):
    def __init__(self, geometries):
        self.geometries = geometries

    async def load(self, aoi):
        return self.geometries, [1.0] * len(self.geometries)


@pytest.mark.asyncio
async def test_the_area_location_chooses_the_forest():
    russia_like = box(27, 41, 180, 82)
    deforestation = DeforestationRequest(
        FixedGeometries([russia_like]), DeforestationForest(tropics_latitude=30)
    )
    request = DeforestationAnalyticsIn(
        aoi={"type": "admin", "ids": ["RUS"]}, start_year="2010", end_year="2024"
    )

    loss = await deforestation.tree_cover_loss_request(request)

    assert loss.forest_filter == "intact_forest"
    assert loss.aoi.ids == ["RUS"]


class MissingGeometries(AoiGeometryRepository):
    async def load(self, aoi):
        raise AssertionError("The global area has no geometry to load")


@pytest.mark.asyncio
async def test_the_global_area_uses_intact_forest():
    deforestation = DeforestationRequest(
        MissingGeometries(), DeforestationForest(tropics_latitude=30)
    )
    request = DeforestationAnalyticsIn(
        aoi={"type": "admin", "ids": ["global"]}, start_year="2010", end_year="2024"
    )

    loss = await deforestation.tree_cover_loss_request(request)

    assert loss.forest_filter == "intact_forest"
