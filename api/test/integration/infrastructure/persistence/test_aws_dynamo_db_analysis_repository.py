import json
import os
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.domain.models.analysis import Analysis
from app.infrastructure.persistence.aws_dynamodb_s3_analysis_repository import (
    AwsDynamoDbS3AnalysisRepository,
)
from app.models.common.analysis import AnalysisStatus
from moto import mock_aws

TEST_CATEGORY = "integration_tests"
DUMMY_UUID = uuid.UUID("c9787f41-b194-4589-ae53-f45ef290ce6f")

# Set up environment variables for moto
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


class TestLoadingAnalysis:
    @pytest.fixture(autouse=True)
    def cleanup_before_each_test(self):
        pass

    @pytest.fixture
    def aws_credentials(self):
        """Mocked AWS Credentials for moto."""
        # Already set via environment variables
        yield

    @pytest.fixture
    def dynamodb_mock(self, aws_credentials):
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

    @pytest.fixture
    def s3_mock(self, aws_credentials):
        """Create a mock S3 bucket."""
        with mock_aws():
            # Create the S3 bucket
            import boto3

            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="gnw-analytics-api-analysis-results")
            yield s3

    @pytest.fixture
    def analysis_repository(self, dynamodb_mock, s3_mock):
        mock_session = MagicMock()

        # Create a mock DynamoDB table with async methods
        mock_table = AsyncMock()
        mock_table.get_item = AsyncMock()
        mock_table.put_item = AsyncMock()

        # Create a mock DynamoDB resource that returns the table
        mock_dynamo_resource = AsyncMock()
        mock_dynamo_resource.Table = AsyncMock(return_value=mock_table)
        mock_dynamo_resource.__aenter__ = AsyncMock(return_value=mock_dynamo_resource)
        mock_dynamo_resource.__aexit__ = AsyncMock(return_value=None)

        # Create a mock S3 client with async methods
        mock_s3_client = AsyncMock()
        mock_s3_client.get_object = AsyncMock()
        mock_s3_client.put_object = AsyncMock()
        mock_s3_client.delete_object = AsyncMock()
        mock_s3_client.__aenter__ = AsyncMock(return_value=mock_s3_client)
        mock_s3_client.__aexit__ = AsyncMock(return_value=None)

        # Set up the session to return our mocked resource and client
        mock_session.resource.return_value = mock_dynamo_resource
        mock_session.client.return_value = mock_s3_client

        # Configure the mock methods to use the actual Moto implementations
        # For DynamoDB
        mock_table.get_item.side_effect = lambda **kwargs: dynamodb_mock.get_item(
            **kwargs
        )
        mock_table.put_item.side_effect = lambda **kwargs: dynamodb_mock.put_item(
            **kwargs
        )

        # For S3
        def mock_s3_get_object(**kwargs):
            response = s3_mock.get_object(**kwargs)
            # Convert the response to be async compatible
            response["Body"] = AsyncMock()
            response["Body"].__aenter__.return_value.read.return_value = json.dumps(
                ["data1", "data2"]
            ).encode("utf-8")
            return response

        mock_s3_client.get_object.side_effect = mock_s3_get_object
        mock_s3_client.put_object.side_effect = lambda **kwargs: s3_mock.put_object(
            **kwargs
        )
        mock_s3_client.delete_object.side_effect = (
            lambda **kwargs: s3_mock.delete_object(**kwargs)
        )

        # Patch the aioboto3 Session to return our mock
        with patch(
            "api.app.infrastructure.persistence.aws_dynamodb_s3_analysis_repository.aioboto3.Session"
        ) as mock_aioboto3:
            mock_aioboto3.return_value = mock_session

            # Create the repository
            repo = AwsDynamoDbS3AnalysisRepository(TEST_CATEGORY)
            yield repo

    @pytest.mark.asyncio
    async def test_analysis_is_empty_if_doesnt_exist(self, analysis_repository):
        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)
        assert analysis_result == Analysis(result=None, metadata=None, status=None)

    @pytest.mark.asyncio
    async def test_store_saved_analysis_for_first_time(self, analysis_repository):
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
    async def test_store_failed_analysis_persists_failed_status(self):
        analysis_repository = AwsDynamoDbS3AnalysisRepository(TEST_CATEGORY)
        await analysis_repository.store_analysis(
            DUMMY_UUID,
            Analysis(
                result=["data1", "data2"],
                metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
                status=AnalysisStatus.failed,
            ),
        )

        analysis_result = await analysis_repository.load_analysis(DUMMY_UUID)

        assert analysis_result == Analysis(
            result=["data1", "data2"],
            metadata={"val1": 12, "val2": "test", "val3": {"key": "value"}},
            status=AnalysisStatus.failed,
        )

    @pytest.mark.asyncio
    async def test_store_analysis_twice_does_not_append_data(self):
        analysis_repository = AwsDynamoDbS3AnalysisRepository(TEST_CATEGORY)
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
    async def test_update_status_from_pending_to_saved(self):
        analysis_repository = AwsDynamoDbS3AnalysisRepository(TEST_CATEGORY)
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
    async def test_update_status_from_pending_to_failed(self):
        analysis_repository = AwsDynamoDbS3AnalysisRepository(TEST_CATEGORY)
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
