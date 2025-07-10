import os

import pytest

import pandas as pd
from fastapi.testclient import TestClient

from api.app.main import app

client = TestClient(app)


class TestDistAnalyticsWithNoPreviousResult:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        dir_path = (
            "/tmp/dist_alerts_analytics_payloads/9c4b9bb5-0ecd-580d-85c9-d9a112a69b59"
        )

        if os.path.exists(dir_path):
            for filename in os.listdir(dir_path):
                file_path = os.path.join(dir_path, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)

        self.test_request = client.post(
            "/v0/land_change/dist_alerts/analytics",
            json={
                "aois": [{"type": "admin", "id": "IDN.24.9"}],
                "start_date": "2024-08-15",
                "end_date": "2024-08-16",
                "intersections": [],
            },
        )

    def test_post_returns_pending_status(self):
        resource = self.test_request.json()

        assert resource["status"] == "pending"

    def test_post_returns_resource_link(self):
        resource = self.test_request.json()

        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/dist_alerts/analytics/9c4b9bb5-0ecd-580d-85c9-d9a112a69b59"
        )

    def test_post_returns_202_accepted_response_code(self):
        response = self.test_request

        assert response.status_code == 202


class TestDistAnalyticsWithPreviousResult:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""

        # first, create the resource file
        client.post(
            "/v0/land_change/dist_alerts/analytics",
            json={
                "aois": [{"type": "admin", "id": "IDN.24.9"}],
                "start_date": "2024-08-15",
                "end_date": "2024-08-16",
                "intersections": [],
            },
        )

        # now, the resource already exists. Post again...
        self.test_request = client.post(
            "/v0/land_change/dist_alerts/analytics",
            json={
                "aois": [{"type": "admin", "id": "IDN.24.9"}],
                "start_date": "2024-08-15",
                "end_date": "2024-08-16",
                "intersections": [],
            },
        )

    def test_post_returns_pending_status(self):
        resource = self.test_request.json()

        assert resource["status"] == "saved"

    def test_post_returns_resource_link(self):
        resource = self.test_request.json()

        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/dist_alerts/analytics/9c4b9bb5-0ecd-580d-85c9-d9a112a69b59"
        )

    def test_post_202_accepted_response_code(self):
        response = self.test_request

        assert response.status_code == 202


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
