"""Unit tests for build output manifest use case."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from metrics_worker.application.use_cases.build_output_manifest import run as build_manifest
from metrics_worker.domain.entities import MetricOutputManifest


@pytest.mark.asyncio
async def test_build_output_manifest():
    """Test building output manifest."""
    run_id = "test-run-123"
    metric_code = "test.metric"
    version_ts = "2025-01-15T10-30-00"
    row_count = 1000
    output_files = ["metrics.parquet"]
    data_prefix = "metrics/test.metric/2025-01-15T10-30-00"

    clock = MagicMock()
    clock.now.return_value = datetime(2025, 1, 15, 10, 30, 0)

    manifest = await build_manifest(
        run_id,
        metric_code,
        version_ts,
        row_count,
        output_files,
        data_prefix,
        clock,
    )

    assert isinstance(manifest, MetricOutputManifest)
    assert manifest.run_id == run_id
    assert manifest.metric_code == metric_code
    assert manifest.version_ts == version_ts
    assert manifest.row_count == row_count
    assert manifest.outputs["data_prefix"] == data_prefix
    assert manifest.outputs["files"] == output_files
    assert manifest.created_at == datetime(2025, 1, 15, 10, 30, 0)

