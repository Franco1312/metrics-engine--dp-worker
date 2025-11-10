"""S3 I/O operations."""

import json

import boto3
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential

from metrics_worker.domain.types import JsonValue
from metrics_worker.infrastructure.config.settings import Settings


class S3IO:
    """S3 I/O operations."""

    def __init__(self, settings: Settings) -> None:
        """Initialize S3 client."""
        self.settings = settings
        self.s3_client = boto3.client("s3", region_name=settings.aws_region)
        self.bucket = settings.aws_s3_bucket

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def get_json(self, key: str) -> dict[str, JsonValue]:
        """Get JSON object from S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            return json.loads(content)
        except ClientError as e:
            raise RuntimeError(f"Failed to read S3 object {key}: {e}") from e

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def put_json(self, key: str, data: dict[str, JsonValue]) -> None:
        """Put JSON object to S3."""
        try:
            content = json.dumps(data, default=str, indent=2)
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=content.encode("utf-8"),
                ContentType="application/json",
            )
        except ClientError as e:
            raise RuntimeError(f"Failed to write S3 object {key}: {e}") from e

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def put_object(self, key: str, body: bytes, content_type: str = "application/octet-stream") -> None:
        """Put object to S3."""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType=content_type,
            )
        except ClientError as e:
            raise RuntimeError(f"Failed to write S3 object {key}: {e}") from e

    async def object_exists(self, key: str) -> bool:
        """Check if object exists in S3."""
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False
