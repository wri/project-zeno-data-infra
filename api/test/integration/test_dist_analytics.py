import json
import os
import time
from pathlib import Path

import pandas as pd
import pytest
import pytest_asyncio
from app.main import app
from asgi_lifespan import LifespanManager
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

client = TestClient(app)


class TestDistAnalyticsPostWithNoPreviousRequest:
    @pytest_asyncio.fixture(autouse=True)
    async def test_request(self):
        """Runs before each test in this class"""
        delete_resource_files("71f40812-2157-5ce2-b654-377e833e5f73")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                test_request = await client.post(
                    "/v0/land_change/dist_alerts/analytics",
                    json={
                        "aoi": {"type": "admin", "ids": ["IDN.24.9"]},
                        "start_date": "2024-08-15",
                        "end_date": "2024-08-16",
                        "intersections": [],
                    },
                )

                yield test_request

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, test_request):
        resource = test_request.json()
        assert resource["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, test_request):
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/dist_alerts/analytics/71f40812-2157-5ce2-b654-377e833e5f73"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, test_request):
        response = test_request
        assert response.status_code == 202


class TestDistAnalyticsPostWhenPreviousRequestStillProcessing:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        dir_path = delete_resource_files("71f40812-2157-5ce2-b654-377e833e5f73")
        write_metadata_file(dir_path)

        # now, the resource is already processing...make another post
        self.test_request = client.post(
            "/v0/land_change/dist_alerts/analytics",
            json={
                "aoi": {"type": "admin", "ids": ["IDN.24.9"]},
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
            == "http://testserver/v0/land_change/dist_alerts/analytics/71f40812-2157-5ce2-b654-377e833e5f73"
        )

    def test_post_202_accepted_response_code(self):
        response = self.test_request
        assert response.status_code == 202


class TestDistAnalyticsPostWhenPreviousRequestComplete:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        dir_path = delete_resource_files("71f40812-2157-5ce2-b654-377e833e5f73")
        write_metadata_file(dir_path)
        write_data_file(dir_path, {})

        # now, the resource is already processing...make another post
        self.test_request = client.post(
            "/v0/land_change/dist_alerts/analytics",
            json={
                "aoi": {"type": "admin", "ids": ["IDN.24.9"]},
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
            == "http://testserver/v0/land_change/dist_alerts/analytics/71f40812-2157-5ce2-b654-377e833e5f73"
        )

    def test_post_202_accepted_response_code(self):
        response = self.test_request
        assert response.status_code == 202


class TestDistAnalyticsGetWithNoPreviousRequest:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        delete_resource_files("71f40812-2157-5ce2-b654-377e833e5f73")

        self.test_request = client.get(
            "/v0/land_change/dist_alerts/analytics/71f40812-2157-5ce2-b654-377e833e5f73"
        )

    def test_returns_404_not_found_response_code(self):
        response = self.test_request
        assert response.status_code == 404


class TestDistAnalyticsGetWithPreviousRequestStillProcessing:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        dir_path = delete_resource_files("71f40812-2157-5ce2-b654-377e833e5f73")
        write_metadata_file(dir_path)

        self.test_request = client.get(
            "/v0/land_change/dist_alerts/analytics/71f40812-2157-5ce2-b654-377e833e5f73"
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
        dir_path = delete_resource_files("71f40812-2157-5ce2-b654-377e833e5f73")
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
            "/v0/land_change/dist_alerts/analytics/71f40812-2157-5ce2-b654-377e833e5f73"
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


class TestDistAnalyticsPostWithMultipleAdminAOIs:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files("ef700a48-b5ef-532f-8bb8-17f507a97ae7")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    "/v0/land_change/dist_alerts/analytics",
                    json={
                        "aoi": {
                            "type": "admin",
                            "ids": ["IDN.24.9", "IDN.14.13", "BRA.1.1"],
                        },
                        "start_date": "2024-08-15",
                        "end_date": "2024-08-16",
                        "intersections": [],
                    },
                )

                yield (request, client)

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert resource["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/dist_alerts/analytics/ef700a48-b5ef-532f-8bb8-17f507a97ae7"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client = setup
        resource_id = test_request.json()["data"]["link"].split("/")[-1]
        data = await retry_getting_resource(resource_id, client)

        expected_df = pd.DataFrame(
            {
                "country": ["IDN", "IDN", "IDN", "IDN", "BRA"],
                "region": [24, 24, 14, 14, 1],
                "subregion": [9, 9, 13, 13, 1],
                "alert_date": [
                    "2024-08-15",
                    "2024-08-15",
                    "2024-08-15",
                    "2024-08-15",
                    "2024-08-15",
                ],
                "confidence": ["high", "low", "high", "low", "high"],
                "value": [1490, 95, 1414, 126, 2091],
            }
        )

        actual_df = pd.DataFrame(data["result"])
        print(actual_df)

        pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)


class TestDistAnalyticsPostWithMultipleKBAAOIs:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files("b089f8dc-51da-58af-aee2-9eea285c0f84")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    "/v0/land_change/dist_alerts/analytics",
                    json={
                        "aoi": {
                            "type": "key_biodiversity_area",
                            "ids": ["18392", "46942", "18407"],
                        },
                        "start_date": "2025-02-01",
                        "end_date": "2025-04-30",
                        "intersections": [],
                    },
                )

                yield (request, client)

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert resource["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/dist_alerts/analytics/b089f8dc-51da-58af-aee2-9eea285c0f84"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client = setup
        resource_id = test_request.json()["data"]["link"].split("/")[-1]
        data = await retry_getting_resource(resource_id, client)

        expected_df = pd.DataFrame(
            {
                "alert_date": [
                    "2025-02-03",
                    "2025-02-03",
                    "2025-02-11",
                    "2025-02-18",
                    "2025-03-30",
                    "2025-02-23",
                    "2025-02-23",
                    "2025-02-23",
                    "2025-03-05",
                    "2025-03-05",
                ],
                "confidence": [
                    "low",
                    "high",
                    "high",
                    "high",
                    "low",
                    "low",
                    "low",
                    "high",
                    "low",
                    "high",
                ],
                "value": [2, 1, 2, 2, 1, 1, 4, 7, 5, 1],
                "key_biodiversity_area": [
                    "18392",
                    "18392",
                    "18392",
                    "18392",
                    "18392",
                    "46942",
                    "18407",
                    "18407",
                    "18407",
                    "18407",
                ],
            }
        )

        actual_df = pd.DataFrame(data["result"])
        print(actual_df)

        pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)


@pytest.mark.asyncio
async def test_gadm_dist_analytics_no_intersection():
    delete_resource_files("71f40812-2157-5ce2-b654-377e833e5f73")

    async with LifespanManager(app):
        async with AsyncClient(
            transport=ASGITransport(app), base_url="http://test"
        ) as client:
            resource = await client.post(
                "/v0/land_change/dist_alerts/analytics",
                json={
                    "aoi": {"type": "admin", "ids": ["IDN.24.9"]},
                    "start_date": "2024-08-15",
                    "end_date": "2024-08-16",
                    "intersections": [],
                },
            )

            resource_id = resource.json()["data"]["link"].split("/")[-1]

            data = await retry_getting_resource(resource_id, client)

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


@pytest.mark.asyncio
async def test_kba_dist_analytics_no_intersection():
    delete_resource_files("6d6095db-9d62-5914-af37-963e6a13c074")

    async with LifespanManager(app):
        async with AsyncClient(
            transport=ASGITransport(app), base_url="http://test"
        ) as client:
            resource = await client.post(
                "/v0/land_change/dist_alerts/analytics",
                json={
                    "aoi": {"type": "key_biodiversity_area", "ids": ["8111"]},
                    "start_date": "2024-08-15",
                    "end_date": "2024-08-16",
                    "intersections": [],
                },
            )

            resource_id = resource.json()["data"]["link"].split("/")[-1]

            data = await retry_getting_resource(resource_id, client)

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
    dir_path = Path(f"/tmp/dist_alerts_analytics_payloads/{resource_id}")

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
                "aoi": {
                    "ids": ["IDN.24.9"],
                    "provider": "gadm",
                    "type": "admin",
                    "version": "4.1",
                },
                "end_date": "2024-08-16",
                "intersections": [],
                "start_date": "2024-08-15",
            }
        )
    )


def write_data_file(dir_path, data):
    data_file = dir_path / "data.json"
    data_file.write_text(json.dumps(data))


async def retry_getting_resource(resource_id: str, client):
    resource = await client.get(f"/v0/land_change/dist_alerts/analytics/{resource_id}")
    data = resource.json()["data"]
    status = data["status"]
    attempts = 1
    while status == "pending" and attempts < 10:
        resp = await client.get(f"/v0/land_change/dist_alerts/analytics/{resource_id}")
        data = resp.json()["data"]
        status = data["status"]
        time.sleep(1)
        attempts += 1
    if attempts >= 10:
        pytest.fail("Resource stuck on 'pending' status")
    return data
