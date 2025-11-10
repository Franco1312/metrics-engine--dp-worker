"""Unit tests for catalog adapter."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from metrics_worker.infrastructure.runtime.catalog_adapter import S3CatalogAdapter


@pytest.mark.asyncio
async def test_get_dataset_manifest():
    """Test getting dataset manifest."""
    manifest_dict = {
        "dataset_id": "test-dataset",
        "outputs": {"data_prefix": "datasets/test-dataset/data"},
    }

    s3_io = AsyncMock()
    s3_io.get_json = AsyncMock(return_value=manifest_dict)

    adapter = S3CatalogAdapter(s3_io)
    result = await adapter.get_dataset_manifest("test-dataset/manifest.json")

    assert result == manifest_dict
    s3_io.get_json.assert_called_once_with("test-dataset/manifest.json")

