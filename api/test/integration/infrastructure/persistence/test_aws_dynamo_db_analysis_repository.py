import uuid

import aioboto3
import pytest
import pytest_asyncio
from moto.server import ThreadedMotoServer

from app.domain.models.analysis import Analysis
from app.infrastructure.persistence.aws_dynamodb_s3_analysis_repository import (
    AwsDynamoDbS3AnalysisRepository,
)
from app.models.common.analysis import AnalysisStatus

TEST_CATEGORY = "integration_tests"
DUMMY_UUID = uuid.UUID("c9787f41-b194-4589-ae53-f45ef290ce6f")


class TestLoadingAnalysis:
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
            TableName="Analyses",
            KeySchema=[{"AttributeName": "resource_id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "resource_id", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        s3.create_bucket(Bucket="gnw-analytics-api-analysis-results")

    @pytest_asyncio.fixture(scope="function")
    async def dynamodb_and_s3(self, moto_server):
        session = aioboto3.Session()
        async with session.client(
            "s3", region_name="us-east-1", endpoint_url=moto_server
        ) as s3_client:
            async with session.resource(
                "dynamodb", region_name="us-east-1", endpoint_url=moto_server
            ) as dynamo:
                dynamodb_table = await dynamo.Table("Analyses")

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
