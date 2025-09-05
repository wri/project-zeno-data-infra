import uuid

import pytest
from app.domain.models.analysis import Analysis
from app.infrastructure.persistence.aws_dynamodb_s3_analysis_repository import (
    AwsDynamoDbS3AnalysisRepository,
)
from app.models.common.analysis import AnalysisStatus
from moto import mock_aws
from moto.server import ThreadedMotoServer

TEST_CATEGORY = "integration_tests"
DUMMY_UUID = uuid.UUID("c9787f41-b194-4589-ae53-f45ef290ce6f")


class TestLoadingAnalysis:
    @pytest.fixture(scope="session")
    def dynamodb_mock(self):
        """Create a mock DynamoDB table."""
        with mock_aws():
            # Create the DynamoDB table
            import boto3

            dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
            table = dynamodb.create_table(
                TableName="Analyses",
                KeySchema=[{"AttributeName": "resource_id", "KeyType": "HASH"}],
                AttributeDefinitions=[
                    {"AttributeName": "resource_id", "AttributeType": "S"}
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            yield table

    @pytest.fixture(scope="session")
    def s3_mock(self):
        """Create a mock S3 bucket."""
        with mock_aws():
            # Create the S3 bucket
            import boto3

            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="gnw-analytics-api-analysis-results")
            yield s3

    @pytest.fixture(scope="session")
    def moto_server(self, dynamodb_mock, s3_mock):
        """A pytest fixture to start and stop the Moto server."""
        # Use port=0 to get a random, available port
        server = ThreadedMotoServer(port=0)
        server.start()
        host, port = server.get_host_and_port()
        yield f"http://{host}:{port}"
        server.stop()

    @pytest.mark.asyncio
    async def test_analysis_is_empty_if_doesnt_exist(self, moto_server):
        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, moto_server
        )
        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)
        assert analysis_result == Analysis(result=None, metadata=None, status=None)

    @pytest.mark.asyncio
    async def test_store_saved_analysis_for_first_time(self, moto_server):
        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, moto_server
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
    async def test_store_failed_analysis_persists_failed_status(self, moto_server):
        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, moto_server
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
    async def test_store_analysis_twice_does_not_append_data(self, moto_server):
        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, moto_server
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
    async def test_update_status_from_pending_to_saved(self, moto_server):
        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, moto_server
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
    async def test_update_status_from_pending_to_failed(self, moto_server):
        analysis_repository = AwsDynamoDbS3AnalysisRepository(
            TEST_CATEGORY, moto_server
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
