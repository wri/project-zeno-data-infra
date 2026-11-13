from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from shapely.geometry import box

from app.dependencies import get_aoi_geometry_repository
from app.domain.repositories.aoi_geometry_repository import (
    AoiGeometryRepository,
    AoiNotFoundError,
)
from app.main import app
from app.models.common.analysis import AnalysisStatus
from app.routers.land_change.tree_cover_loss.tree_cover_loss import (
    create_analysis_service,
)
from app.use_cases.analysis.analysis_service import AnalysisService

client = TestClient(app)
REQUEST = {
    "aoi": {"type": "admin", "ids": ["IDN"]},
    "start_year": "2002",
    "end_year": "2024",
}


class FixedGeometries(AoiGeometryRepository):
    def __init__(self, geometries):
        self.geometries = geometries

    async def load(self, aoi):
        return self.geometries, [1.0] * len(self.geometries)


class MissingGeometries(AoiGeometryRepository):
    async def load(self, aoi):
        raise AoiNotFoundError("AOI ids not found: IDN")


@pytest.fixture
def service():
    service = MagicMock(spec=AnalysisService)
    service.get_status.return_value = AnalysisStatus.pending
    service.resource_thumbprint.return_value = "8a2ab1a4-4ef0-5b5c-9f0c-7d0dfd1b8f5e"
    app.dependency_overrides[create_analysis_service] = lambda: service
    yield service
    app.dependency_overrides.clear()


def test_deforestation_runs_tree_cover_loss_in_the_area_forest(service):
    indonesia_like = box(95, -11, 141, 6)
    app.dependency_overrides[get_aoi_geometry_repository] = lambda: FixedGeometries(
        [indonesia_like]
    )

    response = client.post("/v0/land_change/deforestation/analytics", json=REQUEST)

    assert response.status_code == 202
    loss = service.set_resource_from.call_args[0][0]
    assert loss.forest_filter == "primary_forest"
    assert "tree_cover_loss/analytics/" in response.json()["data"]["link"]


def test_deforestation_of_an_unknown_area_is_not_found(service):
    app.dependency_overrides[get_aoi_geometry_repository] = MissingGeometries

    response = client.post("/v0/land_change/deforestation/analytics", json=REQUEST)

    assert response.status_code == 404
    service.set_resource_from.assert_not_called()
