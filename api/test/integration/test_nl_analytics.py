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


class TestNLAnalyticsPostWithNoPreviousRequest:
    @pytest_asyncio.fixture(autouse=True)
    async def test_request(self):
        """Runs before each test in this class"""
        delete_resource_files("e5431188-e85e-5893-8ed7-96baa895e21c")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                test_request = await client.post(
                    "/v0/land_change/natural_lands/analytics",
                    json={
                        "aoi": {"type": "admin", "ids": ["IDN.24.9"]},
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
            == "http://testserver/v0/land_change/natural_lands/analytics/e5431188-e85e-5893-8ed7-96baa895e21c"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, test_request):
        response = test_request
        assert response.status_code == 202


class TestNLAnalyticsPostWhenPreviousRequestStillProcessing:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        dir_path = delete_resource_files("e5431188-e85e-5893-8ed7-96baa895e21c")
        write_metadata_file(dir_path)

        # now, the resource is already processing...make another post
        self.test_request = client.post(
            "/v0/land_change/natural_lands/analytics",
            json={
                "aoi": {"type": "admin", "ids": ["IDN.24.9"]},
            },
        )

    def test_post_returns_pending_status(self):
        resource = self.test_request.json()
        assert resource["status"] == "pending"

    def test_post_returns_resource_link(self):
        resource = self.test_request.json()
        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/natural_lands/analytics/e5431188-e85e-5893-8ed7-96baa895e21c"
        )

    def test_post_202_accepted_response_code(self):
        response = self.test_request
        assert response.status_code == 202


class TestNLAnalyticsPostWhenPreviousRequestComplete:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        dir_path = delete_resource_files("e5431188-e85e-5893-8ed7-96baa895e21c")
        write_metadata_file(dir_path)
        write_data_file(dir_path, {})

        # now, the resource is already processing...make another post
        self.test_request = client.post(
            "/v0/land_change/natural_lands/analytics",
            json={
                "aoi": {"type": "admin", "ids": ["IDN.24.9"]},
            },
        )

    def test_post_returns_saved_status(self):
        resource = self.test_request.json()
        assert resource["status"] == "saved"

    def test_post_returns_resource_link(self):
        resource = self.test_request.json()
        assert (
            resource["data"]["link"]
            == "http://testserver/v0/land_change/natural_lands/analytics/e5431188-e85e-5893-8ed7-96baa895e21c"
        )

    def test_post_202_accepted_response_code(self):
        response = self.test_request
        assert response.status_code == 202


class TestNLAnalyticsGetWithNoPreviousRequest:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        delete_resource_files("e5431188-e85e-5893-8ed7-96baa895e21c")

        self.test_request = client.get(
            "/v0/land_change/natural_lands/analytics/e5431188-e85e-5893-8ed7-96baa895e21c"
        )

    def test_returns_404_not_found_response_code(self):
        response = self.test_request
        assert response.status_code == 404


class TestNLAnalyticsGetWithPreviousRequestStillProcessing:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        dir_path = delete_resource_files("e5431188-e85e-5893-8ed7-96baa895e21c")
        write_metadata_file(dir_path)

        self.test_request = client.get(
            "/v0/land_change/natural_lands/analytics/e5431188-e85e-5893-8ed7-96baa895e21c"
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


class TestNLAnalyticsGetWithPreviousRequestComplete:
    @pytest.fixture(autouse=True)
    def setup_before_each(self):
        """Runs before each test in this class"""
        dir_path = delete_resource_files("e5431188-e85e-5893-8ed7-96baa895e21c")
        write_metadata_file(dir_path)
        write_data_file(
            dir_path,
            {
                "country": ["IDN", "IDN"],
                "region": [24, 24],
                "subregion": [9, 9],
                "value": [1490, 95],
            },
        )

        self.test_request = client.get(
            "/v0/land_change/natural_lands/analytics/e5431188-e85e-5893-8ed7-96baa895e21c"
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
                "value": [1490, 95],
            }
        )

        actual_df = pd.DataFrame(self.test_request.json()["data"]["result"])
        pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)

    def test_returns_200_Ok_response_code(self):
        response = self.test_request
        assert response.status_code == 200


class TestNLAnalyticsPostWithMultipleAdminAOIs:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files("71dd3afc-167b-519d-81d9-0f3d2403fd9a/")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    "/v0/land_change/natural_lands/analytics",
                    json={
                        "aoi": {
                            "type": "admin",
                            "ids": ["IDN.24.9", "IDN.14.13", "BRA.1.1"],
                        },
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
            == "http://testserver/v0/land_change/natural_lands/analytics/71dd3afc-167b-519d-81d9-0f3d2403fd9a"
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
                "value": [3.088260e+09, 1.714705e+10, 5.352373e+08],
                "aoi_id": ["IDN.24.9", "IDN.14.13", "BRA.1.1"],
                "aoi_type": ["admin", "admin", "admin"],
            }
        )

        actual_df = pd.DataFrame(data["result"])
        print(actual_df)

        pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)


class TestNLAnalyticsPostWithMultipleKBAAOIs:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        delete_resource_files("3fbf5923-0d61-5f88-b18c-6673ef6b1d62")

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    "/v0/land_change/natural_lands/analytics",
                    json={
                        "aoi": {
                            "type": "key_biodiversity_area",
                            "ids": ["18392", "46942", "18407"],
                        },
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
            == "http://testserver/v0/land_change/natural_lands/analytics/3fbf5923-0d61-5f88-b18c-6673ef6b1d62"
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
                "natural_lands_class": [
                    "Natural short vegetation",
                    "Natural water",
                    "Bare",
                    "Wetland natural short vegetation",
                    "Cropland",
                    "Built-up",
                    "Non-natural tree cover",
                    "Natural short vegetation",
                    "Natural water",
                    "Bare",
                    "Wetland natural short vegetation",
                    "Cropland",
                    "Built-up",
                    "Cropland",
                    "Built-up",
                ],
                "natural_lands_area": [
                    1283753.375,
                    537220.6875,
                    755.5975341796875,
                    755.5962524414062,
                    3408339.5,
                    24179.55859375,
                    2266.7939453125,
                    28739.07421875,
                    14369.591796875,
                    756.2982788085938,
                    756.300048828125,
                    540757.5,
                    53697.4140625,
                    222894.53125,
                    982229.875
                ],
                "aoi_type": [
                    "key_biodiversity_area",
                    "key_biodiversity_area",
                    "key_biodiversity_area",
                    "key_biodiversity_area",
                    "key_biodiversity_area",
                    "key_biodiversity_area",
                    "key_biodiversity_area",
                    "key_biodiversity_area",
                    "key_biodiversity_area",
                    "key_biodiversity_area",
                    "key_biodiversity_area",
                    "key_biodiversity_area",
                    "key_biodiversity_area",
                    "key_biodiversity_area",
                    "key_biodiversity_area",
                ],
                "key_biodiversity_area": [
                    "18392",
                    "18392",
                    "18392",
                    "18392",
                    "18392",
                    "18392",
                    "18392",
                    "18407",
                    "18407",
                    "18407",
                    "18407",
                    "18407",
                    "18407",
                    "46942",
                    "46942",
                ],
            }
        )
        actual_df = pd.DataFrame(data["result"])

        pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)


@pytest.mark.asyncio
async def test_gadm_dist_analytics_no_intersection():
    delete_resource_files("e5431188-e85e-5893-8ed7-96baa895e21c")

    async with LifespanManager(app):
        async with AsyncClient(
            transport=ASGITransport(app), base_url="http://test"
        ) as client:
            resource = await client.post(
                "/v0/land_change/natural_lands/analytics",
                json={
                    "aoi": {"type": "admin", "ids": ["IDN.24.9"]},
                },
            )

            resource_id = resource.json()["data"]["link"].split("/")[-1]

            data = await retry_getting_resource(resource_id, client)

    expected_df = pd.DataFrame(
        {
            "value": 3.088260e+09,
            "aoi_id": "IDN.24.9",
            "aoi_type": "admin",
        }, index=[0]
    )

    actual_df = pd.DataFrame(data["result"])

    pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)


@pytest.mark.asyncio
async def test_kba_dist_analytics_no_intersection():
    delete_resource_files("c9375b98-042c-581d-a50a-6b5b8a11c8eb")

    async with LifespanManager(app):
        async with AsyncClient(
            transport=ASGITransport(app), base_url="http://test"
        ) as client:
            resource = await client.post(
                "/v0/land_change/natural_lands/analytics",
                json={
                    "aoi": {"type": "key_biodiversity_area", "ids": ["8111"]},
                },
            )

            resource_id = resource.json()["data"]["link"].split("/")[-1]

            data = await retry_getting_resource(resource_id, client)

    expected_df = pd.DataFrame(
        {
            "natural_lands_class": [
                "Natural short vegetation",
                "Bare",
                "Cropland",
                "Built-up",
            ],
            "natural_lands_area": [
                9463.1279296875,
                630.880126953125,
                56548228.0,
                2698569.5
            ],
            "aoi_type": [
                "key_biodiversity_area",
                "key_biodiversity_area",
                "key_biodiversity_area",
                "key_biodiversity_area",
            ],
            "key_biodiversity_area": [
                "8111",
                "8111",
                "8111",
                "8111",
            ],
        }
    )

    actual_df = pd.DataFrame(data["result"])
    pd.set_option('display.max_columns', 10)
    print(actual_df)

    pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)


##################################################################
# Utility functions for managing test data                       #
# Since we're just beginning, I don't want to move these out,    #
# yet.                                                           #
##################################################################
def delete_resource_files(resource_id: str) -> Path:
    dir_path = Path(f"/tmp/natural_lands_analytics_payloads/{resource_id}")

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
            }
        )
    )


def write_data_file(dir_path, data):
    data_file = dir_path / "data.json"
    data_file.write_text(json.dumps(data))


async def retry_getting_resource(resource_id: str, client):
    resource = await client.get(f"/v0/land_change/natural_lands/analytics/{resource_id}")
    data = resource.json()["data"]
    status = data["status"]
    attempts = 1
    while status == "pending" and attempts < 10:
        resp = await client.get(f"/v0/land_change/natural_lands/analytics/{resource_id}")
        data = resp.json()["data"]
        status = data["status"]
        time.sleep(1)
        attempts += 1
    if attempts >= 10:
        pytest.fail("Resource stuck on 'pending' status")
    return data
