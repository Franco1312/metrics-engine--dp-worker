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


def lag(series: pd.Series, window: int, obs_time_index: pd.DatetimeIndex | None = None) -> pd.Series:
    """Lag operation (shift by calendar days).
    
    Args:
        series: Series with values
        window: Number of calendar days to lag
        obs_time_index: DatetimeIndex with obs_time values. If provided, uses calendar-based lag.
                       If None, falls back to period-based shift for backward compatibility.
    
    Returns:
        Series with lagged values. If obs_time_index is provided, searches for values
        from exactly N days ago (or the closest available date if that day doesn't exist).
    """
    if window < 1:
        raise ExpressionEvaluationError(f"Lag window must be >= 1, got {window}")
    
    # If obs_time_index is provided, use calendar-based lag
    if obs_time_index is not None:
        if not isinstance(obs_time_index, pd.DatetimeIndex):
            raise ExpressionEvaluationError(
                f"obs_time_index must be a DatetimeIndex, got {type(obs_time_index)}"
            )
        
        if len(series) != len(obs_time_index):
            raise ExpressionEvaluationError(
                f"Series length ({len(series)}) must match obs_time_index length ({len(obs_time_index)})"
            )
        
        # Create a DataFrame with obs_time as index for easier lookup
        df = pd.DataFrame({"value": series.values}, index=obs_time_index)
        
        # Calculate target dates (N days ago)
        target_dates = obs_time_index - pd.Timedelta(days=window)
        
        # Use merge_asof to efficiently find the closest value at or before each target date
        # This is much more efficient than iterating (O(n log n) vs O(n²))
        target_df = pd.DataFrame(index=target_dates)
        target_df.index.name = "target_date"
        
        # Merge to find closest value before or at target_date
        # direction='backward' means we want the last value <= target_date
        merged = pd.merge_asof(
            target_df.reset_index().sort_values("target_date"),
            df.reset_index().rename(columns={"obs_time": "target_date"}),
            on="target_date",
            direction="backward",
        )
        
        # Map back to original obs_time_index order
        result_dict = dict(zip(merged["target_date"], merged["value"]))
        result_values = [result_dict.get(target_date, np.nan) for target_date in target_dates]
        
        return pd.Series(result_values, index=obs_time_index)
    
    # Fallback to period-based shift for backward compatibility
    return series.shift(periods=window)

