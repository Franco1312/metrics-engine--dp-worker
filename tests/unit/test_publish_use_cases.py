"""Unit tests for publish use cases."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from metrics_worker.application.use_cases.publish_completed import run_failure, run_success
from metrics_worker.application.use_cases.publish_heartbeat import run as publish_heartbeat
from metrics_worker.application.use_cases.publish_started import run as publish_started


@pytest.mark.asyncio
async def test_publish_started():
    """Test publishing started event."""
    run_id = "test-run-123"
    metric_code = "test.metric"

    event_bus = AsyncMock()
    clock = MagicMock()
    clock.now.return_value = datetime(2025, 1, 15, 10, 30, 0)

    await publish_started(run_id, metric_code, event_bus, clock)

    event_bus.publish_started.assert_called_once()
    call_args = event_bus.publish_started.call_args[0]
    assert call_args[0] == run_id
    assert call_args[1] == metric_code
    assert call_args[2] == datetime(2025, 1, 15, 10, 30, 0)


@pytest.mark.asyncio
async def test_publish_heartbeat():
    """Test publishing heartbeat event."""
    run_id = "test-run-123"
    metric_code = "test.metric"
    progress = 0.5

    event_bus = AsyncMock()
    clock = MagicMock()
    clock.now.return_value = datetime(2025, 1, 15, 10, 30, 0)

    await publish_heartbeat(run_id, metric_code, progress, event_bus, clock)

    event_bus.publish_heartbeat.assert_called_once()
    call_args = event_bus.publish_heartbeat.call_args[0]
    assert call_args[0] == run_id
    assert call_args[1] == metric_code
    assert call_args[2] == progress
    assert call_args[3] == datetime(2025, 1, 15, 10, 30, 0)


@pytest.mark.asyncio
async def test_publish_completed_success():
    """Test publishing completed event (SUCCESS)."""
    run_id = "test-run-123"
    metric_code = "test.metric"
    version_ts = "2025-01-15T10-30-00"
    output_manifest = "metrics/test.metric/2025-01-15T10-30-00/manifest.json"
    row_count = 1000

    event_bus = AsyncMock()

    await run_success(run_id, metric_code, version_ts, output_manifest, row_count, event_bus)

    event_bus.publish_completed.assert_called_once()
    call_kwargs = event_bus.publish_completed.call_args[1]
    assert call_kwargs["run_id"] == run_id
    assert call_kwargs["metric_code"] == metric_code
    assert call_kwargs["status"] == "SUCCESS"
    assert call_kwargs["version_ts"] == version_ts
    assert call_kwargs["output_manifest"] == output_manifest
    assert call_kwargs["row_count"] == row_count


@pytest.mark.asyncio
async def test_publish_completed_failure():
    """Test publishing completed event (FAILURE)."""
    run_id = "test-run-123"
    metric_code = "test.metric"
    error_code = "INPUT_READ_ERROR"
    error_message = "Series not found: ABC"

    event_bus = AsyncMock()

    await run_failure(run_id, metric_code, error_code, error_message, event_bus)

    event_bus.publish_completed.assert_called_once()
    call_kwargs = event_bus.publish_completed.call_args[1]
    assert call_kwargs["run_id"] == run_id
    assert call_kwargs["metric_code"] == metric_code
    assert call_kwargs["status"] == "FAILURE"
    assert call_kwargs.get("version_ts") is None
    assert call_kwargs.get("output_manifest") is None
    assert call_kwargs["error"] == f"{error_code}: {error_message}"

