from fastapi.testclient import TestClient
from api.app.main import get_analytics_result, DistAlertsAnalyticsIn, AdminAreaOfInterest, app
import pytest


client = TestClient(app)


def test_gadm_get_analytics_result():
    aoi = AdminAreaOfInterest(id="IDN.24.9")

    query = DistAlertsAnalyticsIn(
        aois=[aoi],
        start_date="2024-08-15",
        end_date="2024-08-16",
        intersections=[],
    )
    
    resource = client.post(
        "/v0/land_change/dist_alerts/analytics", 
        json={
            "aois": [{"type": "admin", "id": "IDN.24.9"}],
            "start_date": "2024-08-15",
            "end_date": "2024-08-16",
            "intersections": [],
        }
    ).json()
        
    resource_id = resource["data"]["link"].split("/")[-1]

    data = client.get(f"/v0/land_change/dist_alerts/analytics/{resource_id}").json()["data"]
    assert data["result"]["country"] == ["IDN", "IDN"]
    assert data["result"]["region"] == [24, 24]
    assert data["result"]["subregion"] == [9, 9]
    assert data["result"]["alert_date"] == ["2024-08-15T00:00:00", "2024-08-15T00:00:00"]
    assert data["result"]["confidence"] == ["high", "low"]