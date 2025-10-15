from test.integration import (
    delete_resource_files,
    retry_getting_resource,
    write_data_file,
    write_metadata_file,
)

import pandas as pd
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import Depends, Request
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

from api.app.infrastructure.external_services.duck_db_query_service import (
    DuckDbPrecalcQueryService,
)
from app.domain.analyzers.dist_alerts_analyzer import DistAlertsAnalyzer
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.infrastructure.persistence.file_system_analysis_repository import (
    FileSystemAnalysisRepository,
)
from app.main import app
from app.models.common.areas_of_interest import (
    AdminAreaOfInterest,
    KeyBiodiversityAreaOfInterest,
)
from app.models.land_change.dist_alerts import ANALYTICS_NAME, DistAlertsAnalyticsIn
from app.routers.land_change.dist_alerts.dist_alerts import (
    create_analysis_service,
    create_versioned_dist_alerts_data,
    get_analysis_repository,
)
from app.use_cases.analysis.analysis_service import AnalysisService

TEST_VERSION = "v20251011newschema"

client = TestClient(app)


def get_file_system_analysis_repository() -> AnalysisRepository:
    return FileSystemAnalysisRepository(ANALYTICS_NAME)


async def create_test_version_dist_alerts_data(
    request: Request,
    latest_version: str = TEST_VERSION,
) -> DistAlertsAnalyticsIn:
    body_data = await request.json()
    data = DistAlertsAnalyticsIn(**body_data)
    data._version = latest_version
    return data


def create_analysis_service_for_tests(
    request: Request, analysis_repository=Depends(get_file_system_analysis_repository)
) -> AnalysisService:
    return AnalysisService(
        analysis_repository=analysis_repository,
        analyzer=DistAlertsAnalyzer(
            analysis_repository=analysis_repository,
            compute_engine=getattr(request.app.state, "dask_client", None),
            duckdb_query_service=DuckDbPrecalcQueryService(
                table_uri="s3://lcl-analytics/zonal-statistics/dist-alerts/v20251011newschema/admin-dist-alerts.parquet"
            ),
        ),
        event=ANALYTICS_NAME,
    )


class TestDistAnalyticsPostWithNoPreviousRequest:
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Runs before each test in this class"""
        analytics_in = DistAlertsAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"]),
            start_date="2024-08-15",
            end_date="2024-08-16",
            intersections=[],
        )
        analytics_in._version = TEST_VERSION
        app.dependency_overrides[create_analysis_service] = (
            create_analysis_service_for_tests
        )
        app.dependency_overrides[get_analysis_repository] = (
            get_file_system_analysis_repository
        )
        app.dependency_overrides[create_versioned_dist_alerts_data] = (
            create_test_version_dist_alerts_data
        )

        delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())

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
        assert test_request.status_code == 202


class TestDistAnalyticsPostWhenPreviousRequestStillProcessing:
    @pytest.fixture
    def setup(self):
        """Runs before each test in this class"""
        analytics_in = DistAlertsAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"]),
            start_date="2024-08-15",
            end_date="2024-08-16",
            intersections=[],
        )
        analytics_in._version = TEST_VERSION
        app.dependency_overrides[create_analysis_service] = (
            create_analysis_service_for_tests
        )
        app.dependency_overrides[get_analysis_repository] = (
            get_file_system_analysis_repository
        )
        app.dependency_overrides[create_versioned_dist_alerts_data] = (
            create_test_version_dist_alerts_data
        )

        dir_path = delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())
        write_metadata_file(dir_path)

        # now, the resource is already processing...make another post
        test_request = client.post(
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


class TestDistAnalyticsPostWhenPreviousRequestComplete:
    @pytest.fixture
    def setup(self):
        """Runs before each test in this class"""
        analytics_in = DistAlertsAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"]),
            start_date="2024-08-15",
            end_date="2024-08-16",
            intersections=[],
        )
        analytics_in._version = TEST_VERSION

        app.dependency_overrides[create_analysis_service] = (
            create_analysis_service_for_tests
        )
        app.dependency_overrides[get_analysis_repository] = (
            get_file_system_analysis_repository
        )
        app.dependency_overrides[create_versioned_dist_alerts_data] = (
            create_test_version_dist_alerts_data
        )

        dir_path = delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())
        write_metadata_file(dir_path)
        write_data_file(dir_path, {})

        # now, the resource is already processing...make another post
        test_request = client.post(
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


class TestDistAnalyticsGetWithNoPreviousRequest:
    @pytest.fixture
    def setup(self):
        """Runs before each test in this class"""
        analytics_in = DistAlertsAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"]),
            start_date="2024-08-15",
            end_date="2024-08-16",
            intersections=[],
        )
        analytics_in._version = TEST_VERSION

        app.dependency_overrides[create_analysis_service] = (
            create_analysis_service_for_tests
        )
        app.dependency_overrides[get_analysis_repository] = (
            get_file_system_analysis_repository
        )

        delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())

        test_request = client.get(
            f"/v0/land_change/{ANALYTICS_NAME}/analytics/{analytics_in.thumbprint()}"
        )

        yield test_request, analytics_in

    def test_returns_404_not_found_response_code(self, setup):
        test_request, _ = setup
        response = test_request
        assert response.status_code == 404


class TestDistAnalyticsGetWithPreviousRequestStillProcessing:
    @pytest.fixture
    def setup(self):
        """Runs before each test in this class"""
        analytics_in = DistAlertsAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"]),
            start_date="2024-08-15",
            end_date="2024-08-16",
            intersections=[],
        )
        analytics_in._version = TEST_VERSION

        app.dependency_overrides[create_analysis_service] = (
            create_analysis_service_for_tests
        )
        app.dependency_overrides[get_analysis_repository] = (
            get_file_system_analysis_repository
        )
        app.dependency_overrides[create_versioned_dist_alerts_data] = (
            create_test_version_dist_alerts_data
        )

        dir_path = delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())
        write_metadata_file(dir_path)

        test_request = client.get(
            f"/v0/land_change/{ANALYTICS_NAME}/analytics/{analytics_in.thumbprint()}"
        )

        yield test_request, analytics_in

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


class TestDistAnalyticsGetWithPreviousRequestComplete:
    @pytest.fixture
    def setup(self):
        """Runs before each test in this class"""
        analytics_in = DistAlertsAnalyticsIn(
            aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"]),
            start_date="2024-08-15",
            end_date="2024-08-16",
            intersections=[],
        )
        analytics_in._version = TEST_VERSION

        app.dependency_overrides[create_analysis_service] = (
            create_analysis_service_for_tests
        )
        app.dependency_overrides[get_analysis_repository] = (
            get_file_system_analysis_repository
        )
        app.dependency_overrides[create_versioned_dist_alerts_data] = (
            create_test_version_dist_alerts_data
        )

        dir_path = delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())
        write_metadata_file(dir_path)
        write_data_file(
            dir_path,
            {
                "country": ["IDN", "IDN"],
                "region": [24, 24],
                "subregion": [9, 9],
                "dist_alert_date": ["2024-08-15", "2024-08-15"],
                "dist_alert_confidence": ["high", "low"],
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
                "dist_alert_date": [
                    "2024-08-15",
                    "2024-08-15",
                ],
                "dist_alert_confidence": ["high", "low"],
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


class TestDistAnalyticsPostWithMultipleAdminAOIs:
    @pytest_asyncio.fixture
    async def setup(self):
        """Runs before each test in this class"""
        analytics_in = DistAlertsAnalyticsIn(
            aoi=AdminAreaOfInterest(
                type="admin", ids=["IDN.24.9", "IDN.14.13", "BRA.1.1"]
            ),
            start_date="2024-08-15",
            end_date="2024-08-16",
            intersections=[],
        )
        analytics_in._version = TEST_VERSION
        analytics_in._version = TEST_VERSION
        app.dependency_overrides[create_analysis_service] = (
            create_analysis_service_for_tests
        )
        app.dependency_overrides[get_analysis_repository] = (
            get_file_system_analysis_repository
        )
        app.dependency_overrides[create_versioned_dist_alerts_data] = (
            create_test_version_dist_alerts_data
        )

        delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())

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
        data = await retry_getting_resource(
            ANALYTICS_NAME, analysis_params.thumbprint(), client
        )

        assert len(data["result"].keys()) == 8
        assert "2024-08-15" in data["result"]["dist_alert_date"]
        assert "low" in data["result"]["dist_alert_confidence"]
        assert "high" in data["result"]["dist_alert_confidence"]
        assert "IDN.24.9" in data["result"]["aoi_id"]
        assert "IDN.14.13" in data["result"]["aoi_id"]
        assert "BRA.1.1" in data["result"]["aoi_id"]


class TestDistAnalyticsPostWithMultipleKBAAOIs:
    @pytest_asyncio.fixture
    async def setup(self):
        """Runs before each test in this class"""
        analytics_in = DistAlertsAnalyticsIn(
            aoi=KeyBiodiversityAreaOfInterest(
                type="key_biodiversity_area",
                ids=["18392", "46942", "18407"],
            ),
            start_date="2025-02-01",
            end_date="2025-04-30",
            intersections=[],
        )
        analytics_in._version = TEST_VERSION
        app.dependency_overrides[create_analysis_service] = (
            create_analysis_service_for_tests
        )
        app.dependency_overrides[get_analysis_repository] = (
            get_file_system_analysis_repository
        )
        app.dependency_overrides[create_versioned_dist_alerts_data] = (
            create_test_version_dist_alerts_data
        )

        delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())

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
        data = await retry_getting_resource(
            ANALYTICS_NAME, analysis_params.thumbprint(), client
        )
        result = pd.DataFrame(data["result"])

        # 1. Validate expected columns
        expected_columns = {
            "dist_alert_date",
            "dist_alert_confidence",
            "area_ha",
            "aoi_id",
            "aoi_type",
        }
        assert set(result.columns) == expected_columns, "Column mismatch"

        # 2. Check data types
        assert result["dist_alert_date"].dtype == object  # Should contain string dates
        assert pd.api.types.is_numeric_dtype(
            result["area_ha"]
        ), "area_ha should be numeric"
        assert result["aoi_id"].dtype == object
        assert result["aoi_type"].dtype == object

        # 3. Validate confidence levels
        valid_confidences = {"high", "low"}
        assert set(result["dist_alert_confidence"].unique()).issubset(
            valid_confidences
        ), "Invalid confidence values"

        # 4. Check date format (YYYY-MM-DD)
        date_pattern = r"^\d{4}-\d{2}-\d{2}$"
        assert (
            result["dist_alert_date"].str.match(date_pattern).all()
        ), "Invalid date format"

        # 5. Validate area values
        assert (result["area_ha"] >= 0).all(), "Negative area values"
        assert result["area_ha"].notna().all(), "Missing area values"

        # 6. Check for required aoi_type values
        assert (
            result["aoi_type"] == "key_biodiversity_area"
        ).all(), "Invalid aoi_type values"

        # 7. Verify no empty values in critical columns
        assert result["aoi_id"].notna().all(), "Missing aoi_id values"
        assert set(result["aoi_id"].unique()).issubset({"18392", "46942", "18407"})
        assert result["dist_alert_date"].notna().all(), "Missing date values"

        # 8. Validate reasonable area ranges (adjust thresholds as needed)
        assert result["area_ha"].max() < 100, "Area values too large"
        assert result["area_ha"].min() >= 0, "Negative area values"

        # Optional: Check approximate row count if expected range is known
        assert len(result) > 0, "DataFrame should not be empty"


@pytest.mark.asyncio
async def test_gadm_dist_analytics_no_intersection():
    analytics_in = DistAlertsAnalyticsIn(
        aoi=AdminAreaOfInterest(type="admin", ids=["IDN.24.9"]),
        start_date="2024-08-15",
        end_date="2024-08-16",
        intersections=[],
    )
    analytics_in._version = "v20251011newschema"  # TEST_VERSION
    app.dependency_overrides[create_analysis_service] = (
        create_analysis_service_for_tests
    )
    app.dependency_overrides[get_analysis_repository] = (
        get_file_system_analysis_repository
    )
    app.dependency_overrides[create_versioned_dist_alerts_data] = (
        create_test_version_dist_alerts_data
    )

    delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())

    async with LifespanManager(app):
        async with AsyncClient(
            transport=ASGITransport(app), base_url="http://test"
        ) as client:
            await client.post(
                f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                json=analytics_in.model_dump(),
            )

            data = await retry_getting_resource(
                ANALYTICS_NAME, analytics_in.thumbprint(), client
            )

    expected_df = pd.DataFrame(
        {
            "aoi_id": ["IDN.24.9", "IDN.24.9"],
            "aoi_type": ["admin"] * 2,
            "dist_alert_date": [
                "2024-08-15",
                "2024-08-15",
            ],
            "dist_alert_confidence": ["high", "low"],
            "area_ha": [111.9355, 7.154635],
        }
    )

    actual_df = pd.DataFrame(data["result"])

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
    analytics_in = DistAlertsAnalyticsIn(
        aoi=KeyBiodiversityAreaOfInterest(
            type="key_biodiversity_area",
            ids=["8111"],
        ),
        start_date="2024-08-15",
        end_date="2024-08-16",
        intersections=[],
    )
    analytics_in._version = TEST_VERSION
    app.dependency_overrides[create_analysis_service] = (
        create_analysis_service_for_tests
    )
    app.dependency_overrides[get_analysis_repository] = (
        get_file_system_analysis_repository
    )
    app.dependency_overrides[create_versioned_dist_alerts_data] = (
        create_test_version_dist_alerts_data
    )

    delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())

    async with LifespanManager(app):
        async with AsyncClient(
            transport=ASGITransport(app), base_url="http://test"
        ) as client:
            await client.post(
                f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                json=analytics_in.model_dump(),
            )

            data = await retry_getting_resource(
                ANALYTICS_NAME, analytics_in.thumbprint(), client
            )

    expected_df = pd.DataFrame(
        {
            "aoi_id": ["8111"],
            "aoi_type": ["key_biodiversity_area"],
            "dist_alert_date": ["2024-08-15"],
            "dist_alert_confidence": ["high"],
            "area_ha": [7.255227245390415],
        }
    )

    actual_df = pd.DataFrame(data["result"])
    print(actual_df)

    pd.testing.assert_frame_equal(
        expected_df,
        actual_df,
        check_dtype=False,
        check_like=True,
        check_exact=False,  # Allow approximate comparison for numbers
        atol=1e-8,  # Absolute tolerance
        rtol=1e-4,  # Relative tolerance
    )


@pytest.mark.asyncio
async def test_admin_dist_analytics_by_grasslands():
    analytics_in = DistAlertsAnalyticsIn(
        aoi=AdminAreaOfInterest(type="admin", ids=["TZA.24.3"]),
        start_date="2024-08-15",
        end_date="2024-08-16",
        intersections=["grasslands"],
    )
    analytics_in._version = TEST_VERSION
    app.dependency_overrides[create_analysis_service] = (
        create_analysis_service_for_tests
    )
    app.dependency_overrides[get_analysis_repository] = (
        get_file_system_analysis_repository
    )
    app.dependency_overrides[create_versioned_dist_alerts_data] = (
        create_test_version_dist_alerts_data
    )

    delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())

    async with LifespanManager(app):
        async with AsyncClient(
            transport=ASGITransport(app), base_url="http://test"
        ) as client:
            await client.post(
                f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                json=analytics_in.model_dump(),
            )

            data = await retry_getting_resource(
                ANALYTICS_NAME, analytics_in.thumbprint(), client
            )

    expected_df = pd.DataFrame(
        {
            "country": ["TZA", "TZA"],
            "region": [24, 24],
            "subregion": [3, 3],
            "grasslands": ["non-grasslands", "non-grasslands"],
            "dist_alert_date": ["2024-08-15", "2024-08-16"],
            "dist_alert_confidence": ["high", "high"],
            "area_ha": [1.9975835938, 0.6147143555],
            "aoi_id": ["TZA.24.3", "TZA.24.3"],
            "aoi_type": ["admin", "admin"],
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
async def test_admin_dist_analytics_by_land_cover():
    analytics_in = DistAlertsAnalyticsIn(
        aoi=AdminAreaOfInterest(type="admin", ids=["TZA.24.3"]),
        start_date="2024-08-15",
        end_date="2024-08-16",
        intersections=["land_cover"],
    )
    analytics_in._version = TEST_VERSION
    app.dependency_overrides[create_analysis_service] = (
        create_analysis_service_for_tests
    )
    app.dependency_overrides[get_analysis_repository] = (
        get_file_system_analysis_repository
    )
    app.dependency_overrides[create_versioned_dist_alerts_data] = (
        create_test_version_dist_alerts_data
    )

    delete_resource_files(ANALYTICS_NAME, analytics_in.thumbprint())

    async with LifespanManager(app):
        async with AsyncClient(
            transport=ASGITransport(app), base_url="http://test"
        ) as client:
            await client.post(
                f"/v0/land_change/{ANALYTICS_NAME}/analytics",
                json=analytics_in.model_dump(),
            )

            data = await retry_getting_resource(
                ANALYTICS_NAME, analytics_in.thumbprint(), client
            )

    expected_df = pd.DataFrame(
        {
            "country": [
                "TZA",
                "TZA",
                "TZA",
                "TZA",
                "TZA",
                "TZA",
            ],
            "region": [
                24,
                24,
                24,
                24,
                24,
                24,
            ],
            "subregion": [
                3,
                3,
                3,
                3,
                3,
                3,
            ],
            "land_cover": [
                "Built-up",
                "Cropland",
                "Short vegetation",
                "Short vegetation",
                "Tree cover",
                "Tree cover",
            ],
            "dist_alert_date": [
                "2024-08-15",
                "2024-08-15",
                "2024-08-15",
                "2024-08-16",
                "2024-08-15",
                "2024-08-16",
            ],
            "dist_alert_confidence": [
                "high",
                "high",
                "high",
                "high",
                "high",
                "high",
            ],
            "area_ha": [
                0.3073667969,
                0.7682643555,
                0.7682792969,
                0.9989411133,
                0.2305148438,
                0.6146835938,
            ],
            "aoi_id": [
                "TZA.24.3",
                "TZA.24.3",
                "TZA.24.3",
                "TZA.24.3",
                "TZA.24.3",
                "TZA.24.3",
            ],
            "aoi_type": [
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
    pd.set_option("display.max_columns", None)
    print(actual_df)

    pd.testing.assert_frame_equal(
        expected_df,
        actual_df,
        check_like=True,
        check_exact=False,  # Allow approximate comparison for numbers
        atol=1e-8,  # Absolute tolerance
        rtol=1e-4,  # Relative tolerance
    )
