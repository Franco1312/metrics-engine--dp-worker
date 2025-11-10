"""Catalog adapter."""

import structlog

from metrics_worker.domain.ports import CatalogPort
from metrics_worker.domain.types import DatasetManifestDict
from metrics_worker.infrastructure.aws.s3_io import S3IO
from metrics_worker.infrastructure.aws.s3_path import S3Path

logger = structlog.get_logger()


class S3CatalogAdapter(CatalogPort):
    """S3-based catalog adapter."""

    def __init__(self, s3_io: S3IO) -> None:
        """Initialize catalog adapter."""
        self.s3_io = s3_io

    async def get_dataset_manifest(self, manifest_path: str) -> DatasetManifestDict:
        """Get dataset manifest from S3."""
        logger.info(
            "getting_manifest",
            manifest_path=manifest_path,
            bucket=self.s3_io.bucket,
        )
        try:
            return await self.s3_io.get_json(manifest_path)
        except RuntimeError as e:
            prefix = S3Path.parent(manifest_path)
            available_files = self._list_objects_with_prefix(prefix)
            
            error_msg = (
                f"Manifest not found: {manifest_path} "
                f"(bucket: {self.s3_io.bucket}). "
            )
            if available_files:
                error_msg += f"Available files with prefix '{prefix}': {available_files[:10]}"
            else:
                error_msg += f"No files found with prefix '{prefix}'"
            
            logger.error(
                "manifest_not_found",
                manifest_path=manifest_path,
                bucket=self.s3_io.bucket,
                prefix=prefix,
                available_files=available_files[:10] if available_files else [],
            )
            raise RuntimeError(error_msg) from e

    def _list_objects_with_prefix(self, prefix: str) -> list[str]:
        """List objects in S3 with given prefix."""
        try:
            response = self.s3_io.s3_client.list_objects_v2(
                Bucket=self.s3_io.bucket,
                Prefix=prefix,
                MaxKeys=20,
            )
            if "Contents" in response:
                return [obj["Key"] for obj in response["Contents"]]
            return []
        except Exception as e:
            logger.warning("failed_to_list_objects", prefix=prefix, error=str(e))
            return []

