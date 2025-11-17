"""Unit tests for validate output manifest use case."""

from datetime import datetime

import pytest

from metrics_worker.application.use_cases.validate_output_manifest import run as validate_manifest
from metrics_worker.domain.entities import MetricOutputManifest
from metrics_worker.domain.errors import ManifestValidationError


@pytest.mark.asyncio
async def test_validate_output_manifest_success():
    """Test validating valid output manifest."""
    manifest = MetricOutputManifest(
        run_id="test-run-123",
        metric_code="test.metric",
        version_ts="2025-01-15T10-30-00",
        created_at=datetime.now(),
        row_count=1000,
        outputs={"data_prefix": "metrics/test.metric/2025-01-15T10-30-00", "files": ["metrics.jsonl"]},
    )

    await validate_manifest(manifest, "test-run-123", "test.metric")


@pytest.mark.asyncio
async def test_validate_output_manifest_wrong_run_id():
    """Test validating manifest with wrong run_id."""
    manifest = MetricOutputManifest(
        run_id="wrong-run-id",
        metric_code="test.metric",
        version_ts="2025-01-15T10-30-00",
        created_at=datetime.now(),
        row_count=1000,
        outputs={"data_prefix": "metrics/test.metric/2025-01-15T10-30-00", "files": ["metrics.jsonl"]},
    )

    with pytest.raises(ManifestValidationError, match="run_id mismatch"):
        await validate_manifest(manifest, "test-run-123", "test.metric")


@pytest.mark.asyncio
async def test_validate_output_manifest_wrong_metric_code():
    """Test validating manifest with wrong metric_code."""
    manifest = MetricOutputManifest(
        run_id="test-run-123",
        metric_code="wrong.metric",
        version_ts="2025-01-15T10-30-00",
        created_at=datetime.now(),
        row_count=1000,
        outputs={"data_prefix": "metrics/test.metric/2025-01-15T10-30-00", "files": ["metrics.jsonl"]},
    )

    with pytest.raises(ManifestValidationError, match="metric_code mismatch"):
        await validate_manifest(manifest, "test-run-123", "test.metric")


@pytest.mark.asyncio
async def test_validate_output_manifest_missing_version_ts():
    """Test validating manifest with missing version_ts."""
    manifest = MetricOutputManifest(
        run_id="test-run-123",
        metric_code="test.metric",
        version_ts="",
        created_at=datetime.now(),
        row_count=1000,
        outputs={"data_prefix": "metrics/test.metric/2025-01-15T10-30-00", "files": ["metrics.jsonl"]},
    )

    with pytest.raises(ManifestValidationError, match="missing version_ts"):
        await validate_manifest(manifest, "test-run-123", "test.metric")


@pytest.mark.asyncio
async def test_validate_output_manifest_negative_row_count():
    """Test validating manifest with negative row_count."""
    manifest = MetricOutputManifest(
        run_id="test-run-123",
        metric_code="test.metric",
        version_ts="2025-01-15T10-30-00",
        created_at=datetime.now(),
        row_count=-1,
        outputs={"data_prefix": "metrics/test.metric/2025-01-15T10-30-00", "files": ["metrics.jsonl"]},
    )

    with pytest.raises(ManifestValidationError, match="Invalid row_count"):
        await validate_manifest(manifest, "test-run-123", "test.metric")


@pytest.mark.asyncio
async def test_validate_output_manifest_missing_data_prefix():
    """Test validating manifest with missing data_prefix."""
    manifest = MetricOutputManifest(
        run_id="test-run-123",
        metric_code="test.metric",
        version_ts="2025-01-15T10-30-00",
        created_at=datetime.now(),
        row_count=1000,
        outputs={"files": ["metrics.jsonl"]},
    )

    with pytest.raises(ManifestValidationError, match="missing outputs.data_prefix"):
        await validate_manifest(manifest, "test-run-123", "test.metric")


@pytest.mark.asyncio
async def test_validate_output_manifest_missing_files():
    """Test validating manifest with missing files."""
    manifest = MetricOutputManifest(
        run_id="test-run-123",
        metric_code="test.metric",
        version_ts="2025-01-15T10-30-00",
        created_at=datetime.now(),
        row_count=1000,
        outputs={"data_prefix": "metrics/test.metric/2025-01-15T10-30-00", "files": []},
    )

    with pytest.raises(ManifestValidationError, match="missing outputs.files"):
        await validate_manifest(manifest, "test-run-123", "test.metric")

