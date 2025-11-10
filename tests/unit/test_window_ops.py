"""Unit tests for window operations."""

import numpy as np
import pandas as pd
import pytest

from metrics_worker.application.services.window_ops import (
    ema,
    lag,
    sma,
    window_max,
    window_min,
    window_sum,
)


def test_sma():
    """Test simple moving average."""
    series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = sma(series, window=3)
    assert len(result) == 5
    assert pd.isna(result.iloc[0])
    assert pd.isna(result.iloc[1])
    assert result.iloc[2] == 2.0
    assert result.iloc[3] == 3.0
    assert result.iloc[4] == 4.0


def test_sma_insufficient_window():
    """Test SMA with insufficient data."""
    series = pd.Series([1.0, 2.0])
    result = sma(series, window=5)
    assert all(np.isnan(result))


def test_ema():
    """Test exponential moving average."""
    series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = ema(series, window=3)
    assert len(result) == 5
    assert not np.isnan(result.iloc[0])


def test_window_sum():
    """Test window sum."""
    series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = window_sum(series, window=3)
    assert result.iloc[2] == 6.0
    assert result.iloc[3] == 9.0
    assert result.iloc[4] == 12.0


def test_window_max():
    """Test window max."""
    series = pd.Series([1.0, 5.0, 3.0, 2.0, 4.0])
    result = window_max(series, window=3)
    assert result.iloc[2] == 5.0
    assert result.iloc[3] == 5.0
    assert result.iloc[4] == 4.0


def test_window_min():
    """Test window min."""
    series = pd.Series([5.0, 1.0, 3.0, 4.0, 2.0])
    result = window_min(series, window=3)
    assert result.iloc[2] == 1.0
    assert result.iloc[3] == 1.0
    assert result.iloc[4] == 2.0


def test_lag():
    """Test lag operation."""
    series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = lag(series, window=2)
    assert np.isnan(result.iloc[0])
    assert np.isnan(result.iloc[1])
    assert result.iloc[2] == 1.0
    assert result.iloc[3] == 2.0
    assert result.iloc[4] == 3.0


def test_invalid_window():
    """Test invalid window size."""
    series = pd.Series([1.0, 2.0, 3.0])
    with pytest.raises(Exception):
        sma(series, window=0)
    with pytest.raises(Exception):
        lag(series, window=-1)

