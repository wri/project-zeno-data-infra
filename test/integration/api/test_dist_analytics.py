import pandas as pd
from fastapi.testclient import TestClient

from api.app.main import app

client = TestClient(app)


def test_gadm_dist_analytics_no_intersection():
    resource = client.post(
        "/v0/land_change/dist_alerts/analytics",
        json={
            "aois": [{"type": "admin", "id": "IDN.24.9"}],
            "start_date": "2024-08-15",
            "end_date": "2024-08-16",
            "intersections": [],
        },
    ).json()

    resource_id = resource["data"]["link"].split("/")[-1]

    data = client.get(f"/v0/land_change/dist_alerts/analytics/{resource_id}").json()[
        "data"
    ]

    expected_df = pd.DataFrame(
        {
            "country": ["IDN", "IDN"],
            "region": [24, 24],
            "subregion": [9, 9],
            "alert_date": [
                "2024-08-15",
                "2024-08-15",
            ],
            "confidence": ["high", "low"],
            "value": [1490, 95],
        }
    )

    actual_df = pd.DataFrame(data["result"])
    print(actual_df)

    pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)


def test_kba_dist_analytics_no_intersection():
    resource = client.post(
        "/v0/land_change/dist_alerts/analytics",
        json={
            "aois": [{"type": "key_biodiversity_area", "id": "8111"}],
            "start_date": "2024-08-15",
            "end_date": "2024-08-16",
            "intersections": [],
        },
    ).json()

    resource_id = resource["data"]["link"].split("/")[-1]

    data = client.get(f"/v0/land_change/dist_alerts/analytics/{resource_id}").json()[
        "data"
    ]

    expected_df = pd.DataFrame(
        {
            "key_biodiversity_area": ["8111"],
            "alert_date": ["2024-08-15"],
            "confidence": ["high"],
            "value": [123],
        }
    )

    actual_df = pd.DataFrame(data["result"])
    print(actual_df)

    pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)
