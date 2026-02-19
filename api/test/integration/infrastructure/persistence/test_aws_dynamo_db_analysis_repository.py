import json
import os
import uuid

import aioboto3
import pytest
import pytest_asyncio
from moto.server import ThreadedMotoServer

from app.domain.models.analysis import Analysis
from app.infrastructure.persistence.aws_dynamodb_s3_analysis_repository import (
    GEOMETRY_S3_PREFIX,
    AwsDynamoDbS3AnalysisRepository,
)
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import CustomAreaOfInterest

TEST_CATEGORY = "integration_tests"
DUMMY_UUID = uuid.UUID("c9787f41-b194-4589-ae53-f45ef290ce6f")
CUSTOM_AOI_UUID = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

SAMPLE_FEATURE_COLLECTION = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "test_poly",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [97.600086, 2.700319],
                        [97.600772, 2.676315],
                        [97.653731, 2.677687],
                        [97.650298, 2.700512],
                        [97.600086, 2.700319],
                    ]
                ],
            },
        }
    ],
}


class TestLoadingAnalysis:
    TEST_ANALYSES_TABLE_NAME = os.getenv("ANALYSES_TABLE_NAME")
    TEST_ANALYSIS_RESULTS_BUCKET_NAME = os.getenv("ANALYSIS_RESULTS_BUCKET_NAME")

    @pytest.fixture(scope="class")
    def moto_server(self):
        """Start Moto server once per test class with isolated port."""
        server = ThreadedMotoServer(port=0)
        server.start()
        host, port = server.get_host_and_port()
        yield f"http://{host}:{port}"
        server.stop()

    @pytest.fixture(scope="class", autouse=True)
    def aws_infrastructure(self, moto_server):
        """Create AWS resources once per test class using direct API calls."""
        import boto3
        from botocore.config import Config

        # Configure clients to use Moto server
        config = Config(
            region_name="us-east-1",
            retries={"max_attempts": 0},  # Disable retries for faster failure
        )

        dynamodb = boto3.resource(
            "dynamodb",
            endpoint_url=moto_server,
            config=config,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            aws_session_token="testing",
        )

        s3 = boto3.client(
            "s3",
            endpoint_url=moto_server,
            config=config,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            aws_session_token="testing",
        )

        # Create resources
        dynamodb.create_table(
            TableName=self.TEST_ANALYSES_TABLE_NAME,
            KeySchema=[{"AttributeName": "resource_id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "resource_id", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        s3.create_bucket(Bucket=self.TEST_ANALYSIS_RESULTS_BUCKET_NAME)

    @pytest_asyncio.fixture(scope="function")
    async def dynamodb_and_s3(self, moto_server):
        session = aioboto3.Session()
        async with session.client(
            "s3", region_name="us-east-1", endpoint_url=moto_server
        ) as s3_client:
            async with session.resource(
                "dynamodb", region_name="us-east-1", endpoint_url=moto_server
            ) as dynamo:
                dynamodb_table = await dynamo.Table(self.TEST_ANALYSES_TABLE_NAME)

                yield dynamodb_table, s3_client, moto_server

    @pytest.mark.asyncio
    async def test_analysis_is_empty_if_doesnt_exist(self, dynamodb_and_s3):
        dynamodb_table, s3_client, moto_server = dynamodb_and_s3

        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, dynamodb_table, s3_client, moto_server
        )

        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)
        assert analysis_result == Analysis(result=None, metadata=None, status=None)

    @pytest.mark.asyncio
    async def test_analysis_loads_a_dict_metadata_value_that_is_not_a_string(
        self, dynamodb_and_s3
    ):
        """This is for legacy compatibility.
        Originally, the metadata values stored in dynamodb were python dicts converted to json.
        DynamoDb does not support storing float values. CustomAOIs use geojson and therefore
        have coordinates that are floats and started causing errors.
        This test makes sure that we can still load older Analytics Resources that were saved as json
        """
        dynamodb_table, s3_client, moto_server = dynamodb_and_s3

        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, dynamodb_table, s3_client, moto_server
        )

        ddb_item = {
            "resource_id": str(DUMMY_UUID),
            "metadata": {"val1": 12, "val2": "test", "val3": {"key": "value"}},
            "status": "pending",
            "s3_result_key": None,
        }

        await dynamodb_table.put_item(Item=ddb_item)

        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)
        assert analysis_result == Analysis(
            result=None,
            metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
            status=AnalysisStatus.pending,
        )

    @pytest.mark.asyncio
    async def test_store_saved_analysis_for_first_time(self, dynamodb_and_s3):
        dynamodb_table, s3_client, moto_server = dynamodb_and_s3

        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, dynamodb_table, s3_client, moto_server
        )

        await analysis_repository.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=["data1", "data2"],
                metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
                status=AnalysisStatus.saved,
            ),
        )

        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)

        assert analysis_result == Analysis(
            result=["data1", "data2"],
            metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
            status=AnalysisStatus.saved,
        )

    @pytest.mark.asyncio
    async def test_store_failed_analysis_persists_failed_status(self, dynamodb_and_s3):
        dynamodb_table, s3_client, moto_server = dynamodb_and_s3

        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, dynamodb_table, s3_client, moto_server
        )

        await analysis_repository.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=None,
                metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
                status=AnalysisStatus.failed,
            ),
        )

        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)

        assert analysis_result == Analysis(
            result=None,
            metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
            status=AnalysisStatus.failed,
        )

    @pytest.mark.asyncio
    async def test_store_initial_analysis_and_load_successfully(self, dynamodb_and_s3):
        dynamodb_table, s3_client, moto_server = dynamodb_and_s3

        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, dynamodb_table, s3_client, moto_server
        )

        await analysis_repository.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=None,
                metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
                status=None,
            ),
        )

        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)

        assert analysis_result == Analysis(
            result=None,
            metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
            status=None,
        )

    @pytest.mark.asyncio
    async def test_store_initial_analysis_and_load_successfully_with_floats(
        self, dynamodb_and_s3
    ):
        dynamodb_table, s3_client, moto_server = dynamodb_and_s3

        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, dynamodb_table, s3_client, moto_server
        )

        await analysis_repository.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=None,
                metadata={"val1": 12.23, "val2": "test", "val3": {"key": "value"}},
                status=None,
            ),
        )

        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)

        assert analysis_result == Analysis(
            result=None,
            metadata={"val1": 12.23, "val2": "test", "val3": {"key": "value"}},
            status=None,
        )

    @pytest.mark.asyncio
    async def test_store_analysis_twice_does_not_append_data(self, dynamodb_and_s3):
        dynamodb_table, s3_client, moto_server = dynamodb_and_s3

        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, dynamodb_table, s3_client, moto_server
        )

        await analysis_repository.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=["data1", "data2"],
                metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
                status=AnalysisStatus.saved,
            ),
        )

        await analysis_repository.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=["data1", "data2"],
                metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
                status=AnalysisStatus.saved,
            ),
        )

        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)

        assert analysis_result == Analysis(
            result=["data1", "data2"],
            metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
            status=AnalysisStatus.saved,
        )

    @pytest.mark.asyncio
    async def test_update_status_from_pending_to_saved(self, dynamodb_and_s3):
        dynamodb_table, s3_client, moto_server = dynamodb_and_s3

        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, dynamodb_table, s3_client, moto_server
        )

        await analysis_repository.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=None,
                metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
                status=AnalysisStatus.pending,
            ),
        )

        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)
        assert analysis_result == Analysis(
            result=None,
            metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
            status=AnalysisStatus.pending,
        )

        await analysis_repository.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=["data1", "data2"],
                metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
                status=AnalysisStatus.saved,
            ),
        )

        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)

        assert analysis_result == Analysis(
            result=["data1", "data2"],
            metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
            status=AnalysisStatus.saved,
        )

    @pytest.mark.asyncio
    async def test_update_status_from_pending_to_failed(self, dynamodb_and_s3):
        dynamodb_table, s3_client, moto_server = dynamodb_and_s3

        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, dynamodb_table, s3_client, moto_server
        )

        await analysis_repository.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=None,
                metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
                status=AnalysisStatus.pending,
            ),
        )

        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)
        assert analysis_result == Analysis(
            result=None,
            metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
            status=AnalysisStatus.pending,
        )

        await analysis_repository.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=None,
                metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
                status=AnalysisStatus.failed,
            ),
        )

        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)

        assert analysis_result == Analysis(
            result=None,
            metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
            status=AnalysisStatus.failed,
        )

    # --- Custom AOI geometry offloading tests ---

    def _make_custom_aoi_metadata(self):
        custom_aoi = CustomAreaOfInterest(
            feature_collection=SAMPLE_FEATURE_COLLECTION,
        )
        return {
            "aoi": custom_aoi.model_dump(),
            "canopy_cover": 30,
        }

    @pytest.mark.asyncio
    async def test_store_and_load_roundtrip_with_custom_aoi(self, dynamodb_and_s3):
        """Storing an analysis with a custom AOI offloads the geometry
        to S3 and hydrates it back on load."""
        dynamodb_table, s3_client, moto_server = dynamodb_and_s3
        repo = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, dynamodb_table, s3_client, moto_server
        )
        metadata = self._make_custom_aoi_metadata()

        await repo.store_analysis(
            CUSTOM_AOI_UUID,
            Analysis(
                result=None,
                metadata=metadata,
                status=AnalysisStatus.pending,
            ),
        )

        loaded = await repo.load_analysis(CUSTOM_AOI_UUID)

        assert loaded.metadata["aoi"]["feature_collection"] == (
            SAMPLE_FEATURE_COLLECTION
        )
        assert "feature_collection_hash" not in loaded.metadata["aoi"]

    @pytest.mark.asyncio
    async def test_geometry_stored_in_s3_under_correct_key(self, dynamodb_and_s3):
        """The geometry file exists in S3 at the expected path."""
        dynamodb_table, s3_client, moto_server = dynamodb_and_s3
        repo = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, dynamodb_table, s3_client, moto_server
        )
        metadata = self._make_custom_aoi_metadata()

        await repo.store_analysis(
            CUSTOM_AOI_UUID,
            Analysis(
                result=None,
                metadata=metadata,
                status=None,
            ),
        )

        custom_aoi = CustomAreaOfInterest(
            feature_collection=SAMPLE_FEATURE_COLLECTION,
        )
        expected_hash = custom_aoi.compute_geometry_hash()
        expected_key = f"{GEOMETRY_S3_PREFIX}{expected_hash}.geojson"

        response = await s3_client.get_object(
            Bucket=self.TEST_ANALYSIS_RESULTS_BUCKET_NAME,
            Key=expected_key,
        )
        async with response["Body"] as stream:
            body = json.loads(await stream.read())

        assert body == SAMPLE_FEATURE_COLLECTION

    @pytest.mark.asyncio
    async def test_dynamo_item_has_hash_not_geometry(self, dynamodb_and_s3):
        """DynamoDB metadata stores the hash, not the full geometry."""
        dynamodb_table, s3_client, moto_server = dynamodb_and_s3
        repo = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, dynamodb_table, s3_client, moto_server
        )
        metadata = self._make_custom_aoi_metadata()

        await repo.store_analysis(
            CUSTOM_AOI_UUID,
            Analysis(
                result=None,
                metadata=metadata,
                status=None,
            ),
        )

        ddb_response = await dynamodb_table.get_item(
            Key={"resource_id": str(CUSTOM_AOI_UUID)},
        )
        stored_metadata = json.loads(ddb_response["Item"]["metadata"])
        aoi = stored_metadata["aoi"]

        assert "feature_collection_hash" in aoi
        assert "feature_collection" not in aoi

    @pytest.mark.asyncio
    async def test_duplicate_geometry_is_not_reuploaded(self, dynamodb_and_s3):
        """Storing the same geometry twice should not fail
        (head_object finds existing object)."""
        dynamodb_table, s3_client, moto_server = dynamodb_and_s3
        repo = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, dynamodb_table, s3_client, moto_server
        )
        metadata = self._make_custom_aoi_metadata()
        second_uuid = uuid.UUID("b2c3d4e5-f6a7-8901-bcde-f12345678901")

        await repo.store_analysis(
            CUSTOM_AOI_UUID,
            Analysis(result=None, metadata=metadata, status=None),
        )
        await repo.store_analysis(
            second_uuid,
            Analysis(result=None, metadata=metadata, status=None),
        )

        loaded1 = await repo.load_analysis(CUSTOM_AOI_UUID)
        loaded2 = await repo.load_analysis(second_uuid)
        assert (
            loaded1.metadata["aoi"]["feature_collection"] == SAMPLE_FEATURE_COLLECTION
        )
        assert (
            loaded2.metadata["aoi"]["feature_collection"] == SAMPLE_FEATURE_COLLECTION
        )

    @pytest.mark.asyncio
    async def test_non_custom_aoi_metadata_unchanged(self, dynamodb_and_s3):
        """Admin AOI metadata passes through without modification."""
        dynamodb_table, s3_client, moto_server = dynamodb_and_s3
        repo = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, dynamodb_table, s3_client, moto_server
        )

        admin_metadata = {
            "aoi": {
                "type": "admin",
                "ids": ["BRA.12.3"],
                "provider": "gadm",
                "version": "4.1",
            },
            "canopy_cover": 30,
        }

        await repo.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=None,
                metadata=admin_metadata,
                status=AnalysisStatus.pending,
            ),
        )

        loaded = await repo.load_analysis(DUMMY_UUID)
        assert loaded.metadata == admin_metadata
