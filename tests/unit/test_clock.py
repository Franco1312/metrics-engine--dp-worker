"""Unit tests for clock."""

from datetime import datetime

import pytest

from metrics_worker.infrastructure.runtime.clock import SystemClock


def test_now():
    """Test getting current time."""
    clock = SystemClock()
    now = clock.now()

    assert isinstance(now, datetime)


def test_format_version_ts():
    """Test formatting version timestamp."""
    clock = SystemClock()
    dt = datetime(2025, 1, 15, 10, 30, 0)
    version_ts = clock.format_version_ts(dt)

    assert version_ts == "2025-01-15T10-30-00"


def test_format_version_ts_with_seconds():
    """Test formatting version timestamp with seconds."""
    clock = SystemClock()
    dt = datetime(2025, 1, 15, 10, 30, 45)
    version_ts = clock.format_version_ts(dt)

    assert version_ts == "2025-01-15T10-30-45"

