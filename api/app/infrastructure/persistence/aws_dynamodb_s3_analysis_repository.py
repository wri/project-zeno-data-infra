import asyncio
import json
import logging
import os
import traceback
import uuid

from botocore.exceptions import ClientError

from app.analysis.common.analysis import EnumEncoder
from app.domain.models.analysis import Analysis
from app.domain.repositories.analysis_repository import AnalysisRepository
from app.models.common.analysis import AnalysisStatus
from app.models.common.areas_of_interest import CustomAreaOfInterest

RESULTS_BUCKET_NAME = os.getenv("ANALYSIS_RESULTS_BUCKET_NAME", None)
GEOMETRY_S3_PREFIX = "custom_areas/"


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
    def __init__(
        self,
        analytics_category: str,
        dynamo_db_table,
        s3,
        aws_endpoint_url: str | None = None,
    ):
        self.analytics_category = analytics_category
        self._dynamo_db_table = dynamo_db_table
        self._s3 = s3
        self._aws_endpoint_url = aws_endpoint_url
        self._bucket_name = RESULTS_BUCKET_NAME

    def _get_s3_key(self, resource_id: uuid.UUID) -> str:
        """Generates the S3 key for storing the result of a given resource_id."""
        return f"{self.analytics_category}/{resource_id}.json"

    @staticmethod
    def _get_geometry_s3_key(geometry_hash: str) -> str:
        return f"{GEOMETRY_S3_PREFIX}{geometry_hash}.geojson"

    async def _store_geometry(self, geometry: dict, geometry_hash: str):
        """Upload feature_collection to S3, skipping if it already exists."""
        s3_key = self._get_geometry_s3_key(geometry_hash)
        try:
            await _retry_on_throttling(
                self._s3.head_object,
                Bucket=self._bucket_name,
                Key=s3_key,
            )
        except ClientError:
            await _retry_on_throttling(
                self._s3.put_object,
                Bucket=self._bucket_name,
                Key=s3_key,
                Body=json.dumps(geometry).encode("utf-8"),
                ContentType="application/geo+json",
            )

    async def _load_geometry(self, geometry_hash: str) -> dict:
        """Retrieve a feature_collection from S3 by its hash."""
        s3_key = self._get_geometry_s3_key(geometry_hash)
        try:
            response = await _retry_on_throttling(
                self._s3.get_object,
                Bucket=self._bucket_name,
                Key=s3_key,
            )
            async with response["Body"] as stream:
                content = await stream.read()
                return json.loads(content)
        except ClientError as e:
            logging.error(
                {
                    "event": "load_geometry_failed",
                    "severity": "high",
                    "geometry_hash": geometry_hash,
                    "error_type": e.__class__.__name__,
                    "error_details": str(e),
                    "traceback": traceback.format_exc(),
                }
            )
            raise

    async def load_analysis(self, resource_id: uuid.UUID) -> Analysis:
        try:
            # Use consistent read for strong consistency
            response = await _retry_on_throttling(
                self._dynamo_db_table.get_item,
                Key={"resource_id": str(resource_id)},
                ConsistentRead=False,
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

        metadata_value = item.get("metadata")
        metadata = (
            json.loads(metadata_value)
            if isinstance(metadata_value, str)
            else metadata_value
        )
        status_value = item.get("status")
        status = AnalysisStatus(status_value) if status_value else None
        s3_key = item.get("s3_result_key")  # This is the pointer to the result in S3

        # Hydrate custom AOI geometry from S3 if stored as a reference
        if metadata and isinstance(metadata.get("aoi"), dict):
            aoi = metadata["aoi"]
            geometry_hash = aoi.get("feature_collection_hash")
            if geometry_hash and "feature_collection" not in aoi:
                fc = await self._load_geometry(geometry_hash)
                metadata["aoi"]["feature_collection"] = fc
                del metadata["aoi"]["feature_collection_hash"]

        result_payload = None
        # Only try to fetch from S3 if a key exists and status suggests there's a result
        if s3_key and status == AnalysisStatus.saved:
            try:
                response = await _retry_on_throttling(
                    self._s3.get_object, Bucket=self._bucket_name, Key=s3_key
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

        logging.info(
            {
                "event": "aws_dynamodb_s3_analysis_repository",
                "message": "loaded analysis resource",
                "resource_id": resource_id,
                "metadata": metadata,
                "status": status,
            }
        )

        return Analysis(result=result_payload, metadata=metadata, status=status)

    async def store_analysis(self, resource_id: uuid.UUID, analytics: Analysis):
        s3_key = self._get_s3_key(resource_id)
        resource_id_str = str(resource_id)

        # Offload custom AOI feature_collection to S3 before storing metadata
        metadata = analytics.metadata
        if metadata and isinstance(metadata.get("aoi"), dict):
            aoi = metadata["aoi"]
            if aoi.get("type") == "feature_collection" and "feature_collection" in aoi:
                custom_aoi = CustomAreaOfInterest(**aoi)
                geometry_hash = custom_aoi.compute_geometry_hash()

                feature_collection = aoi.get("feature_collection")
                await self._store_geometry(feature_collection, geometry_hash)
                new_aoi = {k: v for k, v in aoi.items() if k != "feature_collection"}
                new_aoi["feature_collection_hash"] = geometry_hash
                metadata = {**metadata, "aoi": new_aoi}

        # Prepare the item for DynamoDB
        # DynamoDB does not allow floats like coordinates found in CustomAOIs.
        # Therefore, 'metadata' is stored as a JSON string.
        ddb_item = {
            "resource_id": resource_id_str,
            "metadata": json.dumps(metadata, cls=EnumEncoder),
            "status": (
                analytics.status.value if analytics.status else None
            ),  # Store the enum's value (string)
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
            await _retry_on_throttling(
                self._s3.put_object,
                Bucket=self._bucket_name,
                Key=s3_key,
                Body=json.dumps(analytics.result).encode("utf-8"),
            )
        elif analytics.status != AnalysisStatus.saved:
            # If status is not 'saved', we should delete any existing result in S3
            # to avoid orphaned data and ensure consistency.
            try:
                await _retry_on_throttling(
                    self._s3.delete_object, Bucket=self._bucket_name, Key=s3_key
                )
            except ClientError as e:
                # It's okay if the object didn't exist. Log other errors.
                if e.response["Error"]["Code"] != "NoSuchKey":
                    raise e

        # Second, store the metadata and pointer in DynamoDB
        await _retry_on_throttling(self._dynamo_db_table.put_item, Item=ddb_item)
