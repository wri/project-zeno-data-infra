import json
import os
from pathlib import Path

import pandas as pd
import pytest
from app.main import app
from fastapi.testclient import TestClient

client = TestClient(app)


class TestDistAnalyticsPostWithNoPreviousRequest:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        delete_resource_files("9c4b9bb5-0ecd-580d-85c9-d9a112a69b59")

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


class TestDistAnalyticsPostWhenPreviousRequestStillProcessing:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        dir_path = delete_resource_files("9c4b9bb5-0ecd-580d-85c9-d9a112a69b59")
        write_metadata_file(dir_path)

        # now, the resource is already processing...make another post
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

    def test_post_202_accepted_response_code(self):
        response = self.test_request
        assert response.status_code == 202


class TestDistAnalyticsPostWhenPreviousRequestComplete:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        dir_path = delete_resource_files("9c4b9bb5-0ecd-580d-85c9-d9a112a69b59")
        write_metadata_file(dir_path)
        write_data_file(dir_path, {})

        # now, the resource is already processing...make another post
        self.test_request = client.post(
            "/v0/land_change/dist_alerts/analytics",
            json={
                "aois": [{"type": "admin", "id": "IDN.24.9"}],
                "start_date": "2024-08-15",
                "end_date": "2024-08-16",
                "intersections": [],
            },
        )

    def test_post_returns_saved_status(self):
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


class TestDistAnalyticsGetWithNoPreviousRequest:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        delete_resource_files("9c4b9bb5-0ecd-580d-85c9-d9a112a69b59")

        self.test_request = client.get(
            "/v0/land_change/dist_alerts/analytics/9c4b9bb5-0ecd-580d-85c9-d9a112a69b59"
        )

    def test_returns_404_not_found_response_code(self):
        response = self.test_request
        assert response.status_code == 404


class TestDistAnalyticsGetWithPreviousRequestStillProcessing:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        dir_path = delete_resource_files("9c4b9bb5-0ecd-580d-85c9-d9a112a69b59")
        write_metadata_file(dir_path)

        self.test_request = client.get(
            "/v0/land_change/dist_alerts/analytics/9c4b9bb5-0ecd-580d-85c9-d9a112a69b59"
        )

    def test_returns_pending_status(self):
        resource = self.test_request.json()
        assert resource["data"]["status"] == "pending"

    def test_returns_retry_after_message(self):
        resource = self.test_request.json()
        assert (
            resource["data"]["message"]
            == "Resource is still processing, follow Retry-After header."
        )

    def test_returns_200_Ok_response_code(self):
        response = self.test_request
        assert response.status_code == 200

    def test_has_a_retry_after_header_set_to_1_second(self):
        headers = self.test_request.headers
        assert headers["Retry-After"] == "1"


class TestDistAnalyticsGetWithPreviousRequestComplete:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        dir_path = delete_resource_files("9c4b9bb5-0ecd-580d-85c9-d9a112a69b59")
        write_metadata_file(dir_path)
        write_data_file(
            dir_path,
            {
                "country": ["IDN", "IDN"],
                "region": [24, 24],
                "subregion": [9, 9],
                "alert_date": ["2024-08-15", "2024-08-15"],
                "confidence": ["high", "low"],
                "value": [1490, 95],
            },
        )

        self.test_request = client.get(
            "/v0/land_change/dist_alerts/analytics/9c4b9bb5-0ecd-580d-85c9-d9a112a69b59"
        )

    def test_returns_saved_status(self):
        resource = self.test_request.json()
        assert resource["data"]["status"] == "saved"

    def test_returns_results(self):
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

        actual_df = pd.DataFrame(self.test_request.json()["data"]["result"])
        pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)

    def test_returns_200_Ok_response_code(self):
        response = self.test_request
        assert response.status_code == 200


def test_gadm_dist_analytics_no_intersection():
    delete_resource_files("9c4b9bb5-0ecd-580d-85c9-d9a112a69b59")

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

    data = retry_getting_resource(resource_id)

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

    pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)


def test_kba_dist_analytics_no_intersection():
    delete_resource_files("ade7d58e-da53-507b-8186-e1b4e78c2ca1")

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

    data = retry_getting_resource(resource_id)

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


##################################################################
# Utility functions for managing test data                       #
# Since we're just beginning, I don't want to move these out,    #
# yet.                                                           #
##################################################################
def delete_resource_files(resource_id: str) -> Path:
    dir_path = Path(
        "/tmp/dist_alerts_analytics_payloads/9c4b9bb5-0ecd-580d-85c9-d9a112a69b59"
    )

    if os.path.exists(dir_path):
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)

    return dir_path


def write_metadata_file(dir_path):
    metadata_file = dir_path / "metadata.json"
    metadata_file.write_text(
        json.dumps(
            {
                "aois": [
                    {
                        "id": "IDN.24.9",
                        "provider": "gadm",
                        "type": "admin",
                        "version": "4.1",
                    }
                ],
                "end_date": "2024-08-16",
                "intersections": [],
                "start_date": "2024-08-15",
            }
        )
    )


def write_data_file(dir_path, data):
    data_file = dir_path / "data.json"
    data_file.write_text(json.dumps(data))


def retry_getting_resource(resource_id: str):
    data = client.get(f"/v0/land_change/dist_alerts/analytics/{resource_id}").json()[
        "data"
    ]
    status = data["status"]
    attempts = 1
    while status == "pending" and attempts < 10:
        data = client.get(
            f"/v0/land_change/dist_alerts/analytics/{resource_id}"
        ).json()["data"]
        status = data["status"]
        attempts += 1
    if attempts >= 10:
        pytest.fail("Resource stuck on 'pending' status")
    return data
