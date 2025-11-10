"""Window operations for metric expressions."""

import numpy as np
import pandas as pd

from metrics_worker.domain.errors import ExpressionEvaluationError


def sma(series: pd.Series, window: int) -> pd.Series:
    """Simple Moving Average."""
    if window < 1:
        raise ExpressionEvaluationError(f"Window must be >= 1, got {window}")
    if len(series) < window:
        return pd.Series(np.nan, index=series.index)
    return series.rolling(window=window, min_periods=window).mean()


def ema(series: pd.Series, window: int) -> pd.Series:
    """Exponential Moving Average."""
    if window < 1:
        raise ExpressionEvaluationError(f"Window must be >= 1, got {window}")
    return series.ewm(span=window, adjust=False).mean()


def window_sum(series: pd.Series, window: int) -> pd.Series:
    """Window sum."""
    if window < 1:
        raise ExpressionEvaluationError(f"Window must be >= 1, got {window}")
    if len(series) < window:
        return pd.Series(np.nan, index=series.index)
    return series.rolling(window=window, min_periods=window).sum()


def window_max(series: pd.Series, window: int) -> pd.Series:
    """Window max."""
    if window < 1:
        raise ExpressionEvaluationError(f"Window must be >= 1, got {window}")
    if len(series) < window:
        return pd.Series(np.nan, index=series.index)
    return series.rolling(window=window, min_periods=window).max()


def window_min(series: pd.Series, window: int) -> pd.Series:
    """Window min."""
    if window < 1:
        raise ExpressionEvaluationError(f"Window must be >= 1, got {window}")
    if len(series) < window:
        return pd.Series(np.nan, index=series.index)
    return series.rolling(window=window, min_periods=window).min()


def lag(series: pd.Series, window: int) -> pd.Series:
    """Lag operation (shift by periods)."""
    if window < 1:
        raise ExpressionEvaluationError(f"Lag window must be >= 1, got {window}")
    return series.shift(periods=window)

