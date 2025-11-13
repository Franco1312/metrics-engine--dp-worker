"""Unit tests for catalog adapter."""

from unittest.mock import AsyncMock

import pytest

from metrics_worker.infrastructure.runtime.catalog_adapter import S3CatalogAdapter


@pytest.mark.asyncio
async def test_get_dataset_manifest():
    """Test getting dataset manifest."""
    manifest_dict = {
        "version_id": "v20251111_014138_730866",
        "dataset_id": "test-dataset",
        "created_at": "2025-11-11T05:13:32Z",
        "collection_date": "2025-11-11T04:41:38Z",
        "data_points_count": 100,
        "series_count": 1,
        "series_codes": ["TEST_SERIES"],
        "date_range": {
            "min_obs_time": "2025-01-01T00:00:00Z",
            "max_obs_time": "2025-11-07T00:00:00Z",
        },
        "parquet_files": ["TEST_SERIES/year=2025/month=11/data.parquet"],
        "partitions": ["TEST_SERIES/year=2025/month=11/"],
        "partition_strategy": "series_year_month",
    }

    s3_io = AsyncMock()
    s3_io.get_json = AsyncMock(return_value=manifest_dict)

    adapter = S3CatalogAdapter(s3_io)
    result = await adapter.get_dataset_manifest("test-dataset/manifest.json")

    assert result == manifest_dict
    s3_io.get_json.assert_called_once_with("test-dataset/manifest.json")

