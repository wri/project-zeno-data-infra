from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from app.main import app
from app.models.common.analysis import AnalysisStatus
from app.routers.land_change.tree_cover_loss.tree_cover_loss import (
    create_analysis_service,
)
from app.use_cases.analysis.analysis_service import AnalysisService

client = TestClient(app)


def test_eudr_runs_tree_cover_loss_in_natural_forest():
    service = MagicMock(spec=AnalysisService)
    service.get_status.return_value = AnalysisStatus.pending
    service.resource_thumbprint.return_value = "8a2ab1a4-4ef0-5b5c-9f0c-7d0dfd1b8f5e"
    app.dependency_overrides[create_analysis_service] = lambda: service

    response = client.post(
        "/v0/land_change/eudr_supply_chain_deforestation/analytics",
        json={
            "aoi": {"type": "admin", "ids": ["GHA"]},
            "start_year": "2021",
            "end_year": "2024",
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 202
    loss = service.set_resource_from.call_args[0][0]
    assert loss.forest_filter == "natural_forest_only"
    assert "tree_cover_loss/analytics/" in response.json()["data"]["link"]
