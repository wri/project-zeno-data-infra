import json
import os
import time
from pathlib import Path

import pandas as pd
import pytest
import pytest_asyncio
from app.domain.analyzers.natural_lands_analyzer import NaturalLandsAnalyzer
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.main import app
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
)
from app.models.land_change.natural_lands import NaturalLandsAnalyticsIn
from app.routers.land_change.natural_lands.natural_lands import (
    ANALYTICS_NAME,
    create_analysis_service,
    get_analysis_repository,
)
from app.use_cases.analysis.analysis_service import AnalysisService
from asgi_lifespan import LifespanManager
from fastapi import Depends, Request
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

client = TestClient(app)


def get_file_system_analysis_repository() -> AnalysisRepository:
    return FileSystemAnalysisRepository(ANALYTICS_NAME)


def create_analysis_service_for_tests(
    request: Request,
    analysis_repository: AnalysisRepository = Depends(
        get_file_system_analysis_repository
    ),
) -> AnalysisService:
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=NaturalLandsAnalyzer(
            analysis_repository=analysis_repository,
            compute_engine=request.app.state.dask_client,
        ),
        event=ANALYTICS_NAME,
    )


class TestNLAnalyticsPostWithNoPreviousRequest:
    @pytest_asyncio.fixture
    async def setup(self):
        analytics_in = NaturalLandsAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"])
        )
        app.dependency_overrides[
            create_analysis_service
        ] = create_analysis_service_for_tests
        app.dependency_overrides[
            get_analysis_repository
        ] = get_file_system_analysis_repository
        delete_resource_files(analytics_in.thumbprint())

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                test_request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json=analytics_in.model_dump(),
                )

                yield test_request, analytics_in

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert resource["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, analysis_params = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == f"http://testserver/v0/land_change/{ANALYTICS_NAME}/analytics/{analysis_params.thumbprint()}"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _ = setup
        response = test_request
        assert response.status_code == 202


class TestNLAnalyticsPostWhenPreviousRequestStillProcessing:
    @pytest_asyncio.fixture
    async def setup(self):
        analytics_in = NaturalLandsAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"])
        )
        app.dependency_overrides[
            create_analysis_service
        ] = create_analysis_service_for_tests
        app.dependency_overrides[
            get_analysis_repository
        ] = get_file_system_analysis_repository

        dir_path = delete_resource_files(analytics_in.thumbprint())
        write_metadata_file(dir_path)

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                test_request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json=analytics_in.model_dump(),
                )

                yield test_request, analytics_in

    def test_post_returns_pending_status(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert resource["status"] == "pending"

    def test_post_returns_resource_link(self, setup):
        test_request, analysis_params = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == f"http://testserver/v0/land_change/{ANALYTICS_NAME}/analytics/{analysis_params.thumbprint()}"
        )

    def test_post_202_accepted_response_code(self, setup):
        test_request, _ = setup
        response = test_request
        assert response.status_code == 202


class TestNLAnalyticsPostWhenPreviousRequestComplete:
    @pytest_asyncio.fixture
    async def setup(self):
        analytics_in = NaturalLandsAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"])
        )
        app.dependency_overrides[
            create_analysis_service
        ] = create_analysis_service_for_tests
        app.dependency_overrides[
            get_analysis_repository
        ] = get_file_system_analysis_repository

        dir_path = delete_resource_files(analytics_in.thumbprint())
        write_metadata_file(dir_path)
        write_data_file(dir_path, {})

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                test_request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json=analytics_in.model_dump(),
                )

                yield test_request, analytics_in

    def test_post_returns_saved_status(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert resource["status"] == "saved"

    def test_post_returns_resource_link(self, setup):
        test_request, analysis_params = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == f"http://testserver/v0/land_change/{ANALYTICS_NAME}/analytics/{analysis_params.thumbprint()}"
        )

    def test_post_202_accepted_response_code(self, setup):
        test_request, _ = setup
        response = test_request
        assert response.status_code == 202


class TestNLAnalyticsGetWithNoPreviousRequest:
    @pytest_asyncio.fixture
    async def setup(self):
        analytics_in = NaturalLandsAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"])
        )
        app.dependency_overrides[
            create_analysis_service
        ] = create_analysis_service_for_tests
        app.dependency_overrides[
            get_analysis_repository
        ] = get_file_system_analysis_repository

        delete_resource_files(analytics_in.thumbprint())

        test_request = client.get(
            f"/v0/land_change/{ANALYTICS_NAME}/analytics/{analytics_in.thumbprint()}"
        )

        yield test_request, analytics_in

    def test_returns_404_not_found_response_code(self, setup):
        test_request, _ = setup
        response = test_request
        assert response.status_code == 404


class TestNLAnalyticsGetWithPreviousRequestStillProcessing:
    @pytest_asyncio.fixture
    async def setup(self):
        analytics_in = NaturalLandsAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"])
        )
        app.dependency_overrides[
            create_analysis_service
        ] = create_analysis_service_for_tests
        app.dependency_overrides[
            get_analysis_repository
        ] = get_file_system_analysis_repository

        dir_path = delete_resource_files(analytics_in.thumbprint())
        write_metadata_file(dir_path)

        self.test_request = client.get(
            f"/v0/land_change/{ANALYTICS_NAME}/analytics/{analytics_in.thumbprint()}"
        )

        yield self.test_request, analytics_in

    def test_returns_pending_status(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert resource["data"]["status"] == "pending"

    def test_returns_retry_after_message(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert (
            resource["data"]["message"]
            == "Resource is still processing, follow Retry-After header."
        )

    def test_returns_200_Ok_response_code(self, setup):
        test_request, _ = setup
        response = test_request
        assert response.status_code == 200

    def test_has_a_retry_after_header_set_to_1_second(self, setup):
        test_request, _ = setup
        headers = test_request.headers
        assert headers["Retry-After"] == "1"


class TestNLAnalyticsGetWithPreviousRequestComplete:
    @pytest_asyncio.fixture
    def setup(self):
        """Runs before each test in this class"""
        analytics_in = NaturalLandsAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"])
        )
        app.dependency_overrides[
            create_analysis_service
        ] = create_analysis_service_for_tests
        app.dependency_overrides[
            get_analysis_repository
        ] = get_file_system_analysis_repository

        dir_path = delete_resource_files(analytics_in.thumbprint())
        write_metadata_file(dir_path)
        write_data_file(
            dir_path,
            {
                "country": ["IDN", "IDN"],
                "region": [24, 24],
                "subregion": [9, 9],
                "area_ha": [1490, 95],
            },
        )

        test_request = client.get(
            f"/v0/land_change/{ANALYTICS_NAME}/analytics/{analytics_in.thumbprint()}"
        )

        yield test_request, analytics_in

    def test_returns_saved_status(self, setup):
        test_request, _ = setup
        resource = test_request.json()
        assert resource["data"]["status"] == "saved"

    def test_returns_results(self, setup):
        test_request, _ = setup
        expected_df = pd.DataFrame(
            {
                "country": ["IDN", "IDN"],
                "region": [24, 24],
                "subregion": [9, 9],
                "area_ha": [1490, 95],
            }
        )

        actual_df = pd.DataFrame(test_request.json()["data"]["result"])
        pd.testing.assert_frame_equal(
            expected_df,
            actual_df,
            check_like=True,
            check_exact=False,  # Allow approximate comparison for numbers
            atol=1e-8,  # Absolute tolerance
            rtol=1e-4,  # Relative tolerance
        )

    def test_returns_200_Ok_response_code(self, setup):
        test_request, _ = setup
        response = test_request
        assert response.status_code == 200


class TestNLAnalyticsPostWithMultipleAdminAOIs:
    @pytest_asyncio.fixture
    async def setup(self):
        analytics_in = NaturalLandsAnalyticsIn(
            aoi=AdminAreaOfInterest(
                type="admin", ids=["IDN.24.9", "IDN.14.13", "BRA.1.1"]
            )
        )
        app.dependency_overrides[
            create_analysis_service
        ] = create_analysis_service_for_tests
        app.dependency_overrides[
            get_analysis_repository
        ] = get_file_system_analysis_repository

        delete_resource_files(analytics_in.thumbprint())

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json=analytics_in.model_dump(),
                )

                yield request, client, analytics_in

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, setup):
        test_request, _, _ = setup
        resource = test_request.json()
        assert resource["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, _, analysis_param = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == f"http://testserver/v0/land_change/{ANALYTICS_NAME}/analytics/{analysis_param.thumbprint()}"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client, analysis_params = setup

        data = await retry_getting_resource(analysis_params.thumbprint(), client)

        expected_df = pd.DataFrame(
            {
                "natural_lands_class": [
                    "Natural forests",
                    "Natural short vegetation",
                    "Natural water",
                    "Bare",
                    "Wetland natural forests",
                    "Natural peat forests",
                    "Wetland natural short vegetation",
                    "Natural peat short vegetation",
                    "Cropland",
                    "Built-up",
                    "Non-natural hhort vegetation",
                    "Non-natural peat short vegetation",
                    "Non-natural bare",
                    "Natural forests",
                    "Natural short vegetation",
                    "Natural water",
                    "Mangroves",
                    "Bare",
                    "Wetland natural forests",
                    "Natural peat forests",
                    "Wetland natural short vegetation",
                    "Natural peat short vegetation",
                    "Cropland",
                    "Built-up",
                    "Non-natural tree cover",
                    "Non-natural water",
                    "Non-natural peat tree cover",
                    "Natural forests",
                    "Natural short vegetation",
                    "Natural water",
                    "Mangroves",
                    "Bare",
                    "Wetland natural forests",
                    "Natural peat forests",
                    "Wetland natural short vegetation",
                    "Natural peat short vegetation",
                    "Cropland",
                    "Built-up",
                    "Non-natural tree cover",
                    "Non-natural water",
                    "Non-natural peat tree cover",
                ],
                "area_ha": [
                    52839.245125971735,
                    70.72704165428877,
                    622.9160351082683,
                    0.5305184572935104,
                    996.7733733206987,
                    994.2030653059483,
                    40.69407768547535,
                    4.091887705028057,
                    74.59025508910418,
                    327.9719085916877,
                    124744.07981751114,
                    133.11662194132805,
                    20.08434423059225,
                    662841.2583017349,
                    130481.95425628126,
                    20561.683858916163,
                    383.61172857135534,
                    4855.239171713591,
                    0.844839908182621,
                    101020.31605447829,
                    93424.85963504761,
                    308265.3662794158,
                    3509.360902108252,
                    3353.419971689582,
                    304339.7195414826,
                    1497.7652883455157,
                    29011.9291607216,
                    33322.009611584246,
                    3988.647222325206,
                    21766.83284316957,
                    4890.2176098152995,
                    974.0232931822538,
                    1.0000538378953934,
                    311080.7036027387,
                    328.26419872790575,
                    5470.66696318984,
                    254557.86518987268,
                    2090.8876435384154,
                    378196.3986384943,
                    45.92507600784302,
                    289961.0399727747,
                ],
                "aoi_id": [
                    "BRA.1.1",
                    "BRA.1.1",
                    "BRA.1.1",
                    "BRA.1.1",
                    "BRA.1.1",
                    "BRA.1.1",
                    "BRA.1.1",
                    "BRA.1.1",
                    "BRA.1.1",
                    "BRA.1.1",
                    "BRA.1.1",
                    "BRA.1.1",
                    "BRA.1.1",
                    "IDN.14.13",
                    "IDN.14.13",
                    "IDN.14.13",
                    "IDN.14.13",
                    "IDN.14.13",
                    "IDN.14.13",
                    "IDN.14.13",
                    "IDN.14.13",
                    "IDN.14.13",
                    "IDN.14.13",
                    "IDN.14.13",
                    "IDN.14.13",
                    "IDN.14.13",
                    "IDN.14.13",
                    "IDN.24.9",
                    "IDN.24.9",
                    "IDN.24.9",
                    "IDN.24.9",
                    "IDN.24.9",
                    "IDN.24.9",
                    "IDN.24.9",
                    "IDN.24.9",
                    "IDN.24.9",
                    "IDN.24.9",
                    "IDN.24.9",
                    "IDN.24.9",
                    "IDN.24.9",
                    "IDN.24.9",
                ],
                "aoi_type": [
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                    "admin",
                ],
            }
        )

        actual_df = pd.DataFrame(data["result"])
        print(actual_df)

        pd.testing.assert_frame_equal(
            expected_df,
            actual_df,
            check_like=True,
            check_exact=False,  # Allow approximate comparison for numbers
            atol=1e-8,  # Absolute tolerance
            rtol=1e-4,  # Relative tolerance
        )


class TestNLAnalyticsPostWithMultipleKBAAOIs:
    @pytest_asyncio.fixture
    async def setup(self):
        analytics_in = NaturalLandsAnalyticsIn(
            aoi=KeyBiodiversityAreaOfInterest(
                type="key_biodiversity_area", ids=["18392", "46942", "18407"]
            )
        )
        app.dependency_overrides[
            create_analysis_service
        ] = create_analysis_service_for_tests
        app.dependency_overrides[
            get_analysis_repository
        ] = get_file_system_analysis_repository

        delete_resource_files(analytics_in.thumbprint())

        async with LifespanManager(app):
            async with AsyncClient(
                transport=ASGITransport(app), base_url="http://testserver"
            ) as client:
                request = await client.post(
                    f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                    json=analytics_in.model_dump(),
                )

                yield request, client, analytics_in

    @pytest.mark.asyncio
    async def test_post_returns_pending_status(self, setup):
        test_request, _, _ = setup
        resource = test_request.json()
        assert resource["status"] == "pending"

    @pytest.mark.asyncio
    async def test_post_returns_resource_link(self, setup):
        test_request, _, analysis_params = setup
        resource = test_request.json()
        assert (
            resource["data"]["link"]
            == f"http://testserver/v0/land_change/{ANALYTICS_NAME}/analytics/{analysis_params.thumbprint()}"
        )

    @pytest.mark.asyncio
    async def test_post_returns_202_accepted_response_code(self, setup):
        test_request, _, _ = setup
        assert test_request.status_code == 202

    @pytest.mark.asyncio
    async def test_resource_calculate_results(self, setup):
        test_request, client, analysis_params = setup
        data = await retry_getting_resource(analysis_params.thumbprint(), client)
        actual_df = pd.DataFrame(data["result"])

        # 1. Validate expected columns
        expected_columns = {"natural_lands_class", "area_ha", "aoi_type", "aoi_id"}
        assert set(actual_df.columns) == expected_columns, "Column mismatch"

        # 2. Check data types
        assert actual_df["natural_lands_class"].dtype == object
        assert pd.api.types.is_numeric_dtype(
            actual_df["area_ha"]
        ), "area_ha should be numeric"
        assert actual_df["aoi_type"].dtype == object
        assert actual_df["aoi_id"].dtype == object

        # 3. Validate aoi_type values
        assert (
            actual_df["aoi_type"] == "key_biodiversity_area"
        ).all(), "Invalid aoi_type values"

        # 4. Check for valid natural land classes (extracted from your example)
        valid_classes = {
            "Bare",
            "Built-up",
            "Cropland",
            "Natural short vegetation",
            "Natural water",
            "Non-natural tree cover",
            "Wetland natural forests",
            "Wetland natural short vegetation",
            "Wetland non-natural short vegetation",
            "Wetland non-natural tree cover",
        }
        assert set(actual_df["natural_lands_class"].unique()).issubset(
            valid_classes
        ), "Invalid natural land classes detected"

        # 5. Validate area values
        assert (actual_df["area_ha"] >= 0).all(), "Negative area values"
        assert actual_df["area_ha"].notna().all(), "Missing area values"

        # Check for reasonable area ranges (adjust based on your domain knowledge)
        assert actual_df["area_ha"].max() < 1000, "Area values unexpectedly large"
        assert actual_df["area_ha"].min() >= 0, "Negative area values"

        # 6. Validate aoi_id format
        # Assuming aoi_id should be numeric strings based on your example
        assert (
            actual_df["aoi_id"].str.isdigit().all()
        ), "aoi_id should contain only digits"
        assert set(actual_df["aoi_id"].unique()).issubset({"18392", "46942", "18407"})
        assert actual_df["aoi_id"].notna().all(), "Missing aoi_id values"

        # 7. Check that each aoi_id has at least one entry
        assert (
            len(actual_df["aoi_id"].unique()) >= 1
        ), "Should have at least one unique aoi_id"

        # 8. Verify no empty values in critical columns
        assert (
            actual_df["natural_lands_class"].notna().all()
        ), "Missing natural_lands_class values"

        # 9. Optional: Check approximate distribution of area values
        # This ensures the data has reasonable statistical properties
        area_mean = actual_df["area_ha"].mean()
        assert 0 < area_mean < 500, f"Unexpected mean area value: {area_mean}"

        # 10. Check that the DataFrame is not empty
        assert len(actual_df) > 0, "DataFrame should not be empty"


@pytest.mark.asyncio
async def test_gadm_dist_analytics_no_intersection():
    analytics_in = NaturalLandsAnalyticsIn(
        aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"])
    )
    app.dependency_overrides[
        create_analysis_service
    ] = create_analysis_service_for_tests
    app.dependency_overrides[
        get_analysis_repository
    ] = get_file_system_analysis_repository

    delete_resource_files(analytics_in.thumbprint())

    async with LifespanManager(app):
        async with AsyncClient(
            transport=ASGITransport(app), base_url="http://test"
        ) as client:
            await client.post(
                f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                json=analytics_in.model_dump(),
            )

            data = await retry_getting_resource(analytics_in.thumbprint(), client)

    expected_df = pd.DataFrame(
        {
            "area_ha": [
                33322.009611584246,
                3988.647222325206,
                21766.83284316957,
                4890.2176098152995,
                974.0232931822538,
                1.0000538378953934,
                311080.7036027387,
                328.26419872790575,
                5470.66696318984,
                254557.86518987268,
                2090.8876435384154,
                378196.3986384943,
                45.92507600784302,
                289961.0399727747,
            ],
            "natural_lands_class": [
                "Natural forests",
                "Natural short vegetation",
                "Natural water",
                "Mangroves",
                "Bare",
                "Wetland natural forests",
                "Natural peat forests",
                "Wetland natural short vegetation",
                "Natural peat short vegetation",
                "Cropland",
                "Built-up",
                "Non-natural tree cover",
                "Non-natural water",
                "Non-natural peat tree cover",
            ],
            "aoi_id": [
                "IDN.24.9",
                "IDN.24.9",
                "IDN.24.9",
                "IDN.24.9",
                "IDN.24.9",
                "IDN.24.9",
                "IDN.24.9",
                "IDN.24.9",
                "IDN.24.9",
                "IDN.24.9",
                "IDN.24.9",
                "IDN.24.9",
                "IDN.24.9",
                "IDN.24.9",
            ],
            "aoi_type": [
                "admin",
                "admin",
                "admin",
                "admin",
                "admin",
                "admin",
                "admin",
                "admin",
                "admin",
                "admin",
                "admin",
                "admin",
                "admin",
                "admin",
            ],
        }
    )

    actual_df = pd.DataFrame(data["result"])
    print(actual_df)

    pd.testing.assert_frame_equal(
        expected_df,
        actual_df,
        check_like=True,
        check_exact=False,  # Allow approximate comparison for numbers
        atol=1e-8,  # Absolute tolerance
        rtol=1e-4,  # Relative tolerance
    )


@pytest.mark.asyncio
async def test_kba_dist_analytics_no_intersection():
    analytics_in = NaturalLandsAnalyticsIn(
        aoi=KeyBiodiversityAreaOfInterest(type="key_biodiversity_area", ids=["8111"])
    )
    app.dependency_overrides[
        create_analysis_service
    ] = create_analysis_service_for_tests
    app.dependency_overrides[
        get_analysis_repository
    ] = get_file_system_analysis_repository

    delete_resource_files(analytics_in.thumbprint())

    async with LifespanManager(app):
        async with AsyncClient(
            transport=ASGITransport(app), base_url="http://test"
        ) as client:
            await client.post(
                f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                json=analytics_in.model_dump(),
            )

            data = await retry_getting_resource(analytics_in.thumbprint(), client)

    expected_df = pd.DataFrame(
        {
            "natural_lands_class": [
                "Natural short vegetation",
                "Bare",
                "Cropland",
                "Built-up",
            ],
            "area_ha": [
                0.94631279296875,
                0.0630880126953125,
                5653.297174409032,
                269.85695,
            ],
            "aoi_type": [
                "key_biodiversity_area",
                "key_biodiversity_area",
                "key_biodiversity_area",
                "key_biodiversity_area",
            ],
            "aoi_id": [
                "8111",
                "8111",
                "8111",
                "8111",
            ],
        }
    )

    actual_df = pd.DataFrame(data["result"])
    pd.set_option("display.max_columns", 10)

    pd.testing.assert_frame_equal(
        expected_df,
        actual_df,
        check_like=True,
        check_exact=False,  # Allow approximate comparison for numbers
        atol=1e-8,  # Absolute tolerance
        rtol=1e-4,  # Relative tolerance
    )


##################################################################
# Utility functions for managing test data                       #
# Since we're just beginning, I don't want to move these out,    #
# yet.                                                           #
##################################################################
def delete_resource_files(resource_id: str) -> Path:
    dir_path = Path(f"/tmp/natural_lands/{resource_id}")

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
    resource = await client.get(
        f"/v0/land_change/natural_lands/analytics/{resource_id}"
    )
    data = resource.json()["data"]
    status = data["status"]
    attempts = 1
    while status == "pending" and attempts < 10:
        resp = await client.get(
            f"/v0/land_change/natural_lands/analytics/{resource_id}"
        )
        data = resp.json()["data"]
        status = data["status"]
        time.sleep(1)
        attempts += 1
    if attempts >= 10:
        pytest.fail("Resource stuck on 'pending' status")
    return data
