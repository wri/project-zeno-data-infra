import asyncio
import json
import logging
import uuid

import aioboto3
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.models.common.analysis import AnalysisStatus
from botocore.exceptions import ClientError


# Helper function for retrying on throttling
async def _retry_on_throttling(operation, *args, max_attempts=3, **kwargs):
    attempts = 0
    while True:
        try:
            return await operation(*args, **kwargs)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if (
                error_code == "ProvisionedThroughputExceededException"
                and attempts < max_attempts
            ):
                attempts += 1
                wait_time = 2**attempts  # Exponential backoff
                await asyncio.sleep(wait_time)
            else:
                raise e  # Re-raise if it's not a throttling error or we've exceeded attempts


class AwsDynamoDbS3AnalysisRepository(AnalysisRepository):
    def __init__(self, analytics_category: str, aws_endpoint_url: str | None = None):
        # Create a session that will be reused. It's resource-efficient.
        self._aws_endpoint_url = aws_endpoint_url
        self._session = aioboto3.Session()
        self._table_name = "Analyses"
        self._aws_region = "us-east-1"
        self._bucket_name = "gnw-analytics-api-analysis-results"
        self.analytics_category = analytics_category

    def _get_s3_key(self, resource_id: uuid.UUID) -> str:
        """Generates the S3 key for storing the result of a given resource_id."""
        return f"{self.analytics_category}/{resource_id}.json"

    async def load_analysis(self, resource_id: uuid.UUID) -> Analysis:
        logging.info(
            {
                "event": "aws_dynamodb_s3_analysis_repository",
                "message": "loading analysis resource",
                "resource_id": resource_id,
            }
        )

        async with self._session.resource(
            "dynamodb",
            region_name=self._aws_region,
            endpoint_url=self._aws_endpoint_url,
        ) as dynamo:
            table = await dynamo.Table(self._table_name)

            try:
                # Use consistent read for strong consistency
                response = await _retry_on_throttling(
                    table.get_item,
                    Key={"resource_id": str(resource_id)},
                    ConsistentRead=True,
                )
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "ResourceNotFoundException":
                    # Table doesn't exist -> return empty analysis
                    return Analysis(result=None, metadata=None, status=None)
                raise e  # Re-raise any other unexpected ClientError

        item = response.get("Item")

        if not item:
            # Item doesn't exist -> return empty analysis
            return Analysis(result=None, metadata=None, status=None)

        # Extract fields from DynamoDB. Use .get() for safety.
        metadata = item.get("metadata")
        status = AnalysisStatus(item.get("status"))
        s3_key = item.get("s3_result_key")  # This is the pointer to the result in S3

        result_payload = None
        # Only try to fetch from S3 if a key exists and status suggests there's a result
        if s3_key and status == AnalysisStatus.saved:
            async with self._session.client(
                "s3", region_name=self._aws_region, endpoint_url=self._aws_endpoint_url
            ) as s3_client:
                try:
                    response = await _retry_on_throttling(
                        s3_client.get_object, Bucket=self._bucket_name, Key=s3_key
                    )
                    async with response["Body"] as stream:
                        content = await stream.read()
                        result_payload = json.loads(content)
                except ClientError as e:
                    error_code = e.response["Error"]["Code"]
                    if error_code != "NoSuchKey":
                        # NoSuchKey is handled silently (result remains None)
                        # Re-raise other errors (e.g., AccessDenied)
                        raise e

        return Analysis(result=result_payload, metadata=metadata, status=status)

    async def store_analysis(self, resource_id: uuid.UUID, analytics: Analysis):
        s3_key = self._get_s3_key(resource_id)
        resource_id_str = str(resource_id)

        # Prepare the item for DynamoDB
        ddb_item = {
            "resource_id": resource_id_str,
            "metadata": analytics.metadata,
            "status": analytics.status.value
            if analytics.status
            else None,  # Store the enum's value (string)
            "s3_result_key": s3_key,  # Always store the pointer, even if result is None
        }

        logging.info(
            {
                "event": "aws_dynamodb_s3_analysis_repository",
                "message": "storing analysis resource",
                "dynamodb_item": ddb_item,
            }
        )

        # First, handle the S3 upload if there is a result and status is 'saved'
        if analytics.result is not None and analytics.status == AnalysisStatus.saved:
            async with self._session.client(
                "s3", region_name=self._aws_region, endpoint_url=self._aws_endpoint_url
            ) as s3_client:
                await _retry_on_throttling(
                    s3_client.put_object,
                    Bucket=self._bucket_name,
                    Key=s3_key,
                    Body=json.dumps(analytics.result).encode("utf-8"),
                )
        elif analytics.status != AnalysisStatus.saved:
            # If status is not 'saved', we should delete any existing result in S3
            # to avoid orphaned data and ensure consistency.
            async with self._session.client(
                "s3", region_name=self._aws_region, endpoint_url=self._aws_endpoint_url
            ) as s3_client:
                try:
                    await _retry_on_throttling(
                        s3_client.delete_object, Bucket=self._bucket_name, Key=s3_key
                    )
                except ClientError as e:
                    # It's okay if the object didn't exist. Log other errors.
                    if e.response["Error"]["Code"] != "NoSuchKey":
                        raise e

        # Second, store the metadata and pointer in DynamoDB
        async with self._session.resource(
            "dynamodb",
            region_name=self._aws_region,
            endpoint_url=self._aws_endpoint_url,
        ) as dynamo:
            table = await dynamo.Table(self._table_name)
            await _retry_on_throttling(table.put_item, Item=ddb_item)
